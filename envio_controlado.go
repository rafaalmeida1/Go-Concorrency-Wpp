package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type Message struct {
	NumeroPedido string
	Celular      string
	Nome         string
	DataEmissao  string
}

// Estruturas para decodificar a resposta da API do Meta
type MetaResponse struct {
	MessagingProduct string           `json:"messaging_product"`
	Contacts         []MetaContact    `json:"contacts"`
	Messages         []MetaMessage    `json:"messages"`
	Error            *MetaErrorDetail `json:"error,omitempty"`
}

type MetaContact struct {
	Input string `json:"input"`
	WaID  string `json:"wa_id"`
}

type MetaMessage struct {
	ID            string `json:"id"`
	MessageStatus string `json:"message_status"`
}

type MetaErrorDetail struct {
	Message   string `json:"message"`
	Type      string `json:"type"`
	Code      int    `json:"code"`
	ErrorData string `json:"error_data"`
	FbTraceID string `json:"fbtrace_id"`
}

// Estrutura para armazenar estatísticas de envio
type Stats struct {
	TotalProcessado    int64
	EnviadosComSucesso int64
	EnviosFalhados     int64
	RateLimitAtingido  int64
	TaxaAtual          int
	IniciadoEm         time.Time
	UltimaAtualizacao  time.Time
	TempoDecorrido     string
}

// Estrutura para receber informações de templates da API do Meta
type TemplateInfo struct {
	Data []struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		Category string `json:"category"`
		Status   string `json:"status"`
	} `json:"data"`
}

var (
	db         *sql.DB
	apiToken   string
	apiPhoneID string
	wabaID     string
	logger     *log.Logger
	logFile    *os.File
	
	// Estatísticas
	enviadosComSucesso int64
	enviosFalhados     int64
	totalProcessado    int64
	rateLimitAtingido  int64
	
	// Controle de taxa
	taxaEnvio          int32 = 50 // Taxa inicial: 50 msgs/seg
	ultimoRateLimit    time.Time
	intervaloEstabilidade = 10 * time.Second // Tempo sem erros para aumentar a taxa
	
	statsUpdateCh = make(chan struct{}, 1)
	startTime     time.Time
	
	// Templates
	templatesUtilidade = []string{
		"transacional_mes_mae",
		"transacional_mes_maes_1",
		"transacional_mes_maes_2",
		"transacional_mes_maes_3",
	}
	
	templatePadrao = "transacional_mes_mae"
	templateToUse  = templatePadrao  // Template que será efetivamente utilizado
	
	// Mapeamento de templates para suas linguagens
	templateLanguages = map[string]string{
		"transacional_mes_mae":    "pt_BR",
		"transacional_mes_maes_1":    "pt_BR",
		"transacional_mes_maes_2":    "pt_BR",
		"transacional_mes_maes_3":    "pt_BR",
	}
	
	templateCategoryCache   = make(map[string]string)
	templateCacheMutex      = sync.Mutex{}
	usarVerificacaoAPI      = true
	terminalLogger          *log.Logger
	
	// Controle de fluxo de mensagens
	messageQueue     chan Message
	semaphore        chan struct{}
	doneChan         chan struct{}
	
	// Mutex para ajuste da taxa
	rateMutex        sync.Mutex
	
	// Mapa para rastrear números já processados
	processedNumbers = sync.Map{}
	
	templateIndex = 0  // Índice para controlar qual template tentar
)

func init() {
	startTime = time.Now()
	setupLogger()
	
	err := godotenv.Load()
	if err != nil {
		logger.Fatalf("Erro ao carregar .env: %v", err)
	}
	
	apiToken = os.Getenv("META_ACCESS_TOKEN")
	apiPhoneID = os.Getenv("META_PHONE_ID")
	wabaID = os.Getenv("META_WABA_ID")
	
	if wabaID == "" {
		logger.Fatalf("META_WABA_ID não definido no arquivo .env")
	}

	dsn := os.Getenv("DB_DSN")
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		logger.Fatalf("Erro ao conectar ao banco de dados: %v", err)
	}
	db.SetMaxOpenConns(50)
	db.SetConnMaxLifetime(time.Minute * 5)
}

func setupLogger() {
	logDir := "logs"
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		os.Mkdir(logDir, 0755)
	}
	
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	logPath := filepath.Join(logDir, fmt.Sprintf("envio_controlado_%s.log", timestamp))
	
	var err error
	logFile, err = os.Create(logPath)
	if err != nil {
		log.Fatalf("Erro ao criar arquivo de log: %v", err)
	}
	
	logger = log.New(logFile, "", log.LstdFlags)
	terminalLogger = log.New(os.Stdout, "", 0)
	terminalLogger.Println("Sistema iniciado. Logs detalhados sendo salvos em:", logPath)
}

func logTerminal(format string, v ...interface{}) {
	logger.Printf(format, v...)
	terminalLogger.Printf(format, v...)
}

func fetchMessages(limit int) ([]Message, error) {
	rows, err := db.Query(`SELECT pedidos_numeroPedido, celular_formatado, pedidos_cliente_nome, pedidos_dataEmissao 
	FROM pedidos_envio_em_massa 
	WHERE (enviada = 0 OR enviada IS NULL) 
	AND pedidos_dataEmissao < '2023-01-01 00:00:00' LIMIT ?`, limit)

	// Buscar apenas números não processados anteriormente
	// rows, err := db.Query(`SELECT DISTINCT pedidos_numeroPedido, celular_formatado, pedidos_cliente_nome, pedidos_dataEmissao 
	// FROM pedidos_envio_em_massa 
	// WHERE (enviada = 0 OR enviada IS NULL) 
	// AND pedidos_dataEmissao >= '2025-05-09 09:30:00' LIMIT ?`, limit)
	
	if err != nil {
		logger.Printf("Erro ao buscar mensagens: %v", err)
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.NumeroPedido, &msg.Celular, &msg.Nome, &msg.DataEmissao); err != nil {
			logger.Printf("Erro ao ler registro: %v", err)
			return nil, err
		}
		
		// Verificar se o número já foi processado
		if _, exists := processedNumbers.Load(msg.Celular); !exists {
			messages = append(messages, msg)
			// Marcar como processado imediatamente
			processedNumbers.Store(msg.Celular, true)
		} else {
			// Número já foi processado, marcar como enviado no banco
			markAsSent(msg.NumeroPedido)
			logger.Printf("Número %s já processado anteriormente, marcando pedido %s como enviado", 
				msg.Celular, msg.NumeroPedido)
		}
	}
	return messages, nil
}

func markAsSent(numeroPedido string) {
	_, err := db.Exec(`UPDATE pedidos_envio_em_massa SET enviada = 1 WHERE pedidos_numeroPedido = ?`, numeroPedido)
	if err != nil {
		logger.Printf("Erro ao marcar como enviada pedido %s: %v", numeroPedido, err)
	}
}

func updateStats() {
	select {
	case statsUpdateCh <- struct{}{}:
	default:
	}
}

func statsMonitor(done <-chan struct{}) {
	saveStats()
	
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			saveStats()
		case <-statsUpdateCh:
			saveStats()
		case <-done:
			saveStats()
			return
		}
	}
}

func saveStats() {
	stats := Stats{
		TotalProcessado:    atomic.LoadInt64(&totalProcessado),
		EnviadosComSucesso: atomic.LoadInt64(&enviadosComSucesso),
		EnviosFalhados:     atomic.LoadInt64(&enviosFalhados),
		RateLimitAtingido:  atomic.LoadInt64(&rateLimitAtingido),
		TaxaAtual:          int(atomic.LoadInt32(&taxaEnvio)),
		IniciadoEm:         startTime,
		UltimaAtualizacao:  time.Now(),
		TempoDecorrido:     time.Since(startTime).Round(time.Second).String(),
	}
	
	statsDir := "stats"
	if _, err := os.Stat(statsDir); os.IsNotExist(err) {
		os.Mkdir(statsDir, 0755)
	}
	
	var sb strings.Builder
	sb.WriteString("=== ESTATÍSTICAS DE ENVIO CONTROLADO ===\n")
	sb.WriteString(fmt.Sprintf("Iniciado em: %s\n", stats.IniciadoEm.Format("02/01/2006 15:04:05")))
	sb.WriteString(fmt.Sprintf("Última atualização: %s\n", stats.UltimaAtualizacao.Format("02/01/2006 15:04:05")))
	sb.WriteString(fmt.Sprintf("Tempo decorrido: %s\n", stats.TempoDecorrido))
	sb.WriteString(fmt.Sprintf("Total processado: %d\n", stats.TotalProcessado))
	sb.WriteString(fmt.Sprintf("Enviados com sucesso: %d\n", stats.EnviadosComSucesso))
	sb.WriteString(fmt.Sprintf("Envios falhados: %d\n", stats.EnviosFalhados))
	sb.WriteString(fmt.Sprintf("Rate limit atingido: %d\n", stats.RateLimitAtingido))
	sb.WriteString(fmt.Sprintf("Taxa atual: %d msgs/seg\n", stats.TaxaAtual))
	
	if stats.TotalProcessado > 0 {
		taxaSucesso := float64(stats.EnviadosComSucesso) / float64(stats.TotalProcessado) * 100
		sb.WriteString(fmt.Sprintf("Taxa de sucesso: %.2f%%\n", taxaSucesso))
	}
	
	jsonStats, _ := json.MarshalIndent(stats, "", "  ")
	
	statsPath := filepath.Join(statsDir, "estatisticas_controlado.txt")
	os.WriteFile(statsPath, []byte(sb.String()), 0644)
	
	jsonStatsPath := filepath.Join(statsDir, "estatisticas_controlado.json")
	os.WriteFile(jsonStatsPath, jsonStats, 0644)
}

// Verifica na API do Meta se o template é de utilidade e retorna também o status
func checkTemplateCategory(templateName string) (string, string, error) {
	templateCacheMutex.Lock()
	if category, exists := templateCategoryCache[templateName]; exists {
		// Não temos o status em cache, então retorna vazio para status
		templateCacheMutex.Unlock()
		return category, "", nil
	}
	templateCacheMutex.Unlock()

	url := fmt.Sprintf("https://graph.facebook.com/v22.0/%s/message_templates?access_token=%s", wabaID, apiToken)

	resp, err := http.Get(url)
	if err != nil {
		return "", "", fmt.Errorf("erro ao consultar API: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("erro ao ler resposta: %v", err)
	}

	if resp.StatusCode != 200 {
		return "", "", fmt.Errorf("API retornou código %d", resp.StatusCode)
	}

	var templateInfo TemplateInfo
	if err := json.Unmarshal(body, &templateInfo); err != nil {
		return "", "", fmt.Errorf("erro ao decodificar JSON: %v", err)
	}

	for _, template := range templateInfo.Data {
		if template.Name == templateName {
			category := template.Category
			status := template.Status

			templateCacheMutex.Lock()
			templateCategoryCache[templateName] = category
			templateCacheMutex.Unlock()

			return category, status, nil
		}
	}

	return "unknown", "", fmt.Errorf("template não encontrado")
}

// Verifica se um template é de utilidade e não está pausado
func isUtilityTemplate(templateName string) (bool, bool) {
	// Verificação na API
	if usarVerificacaoAPI {
		category, status, err := checkTemplateCategory(templateName)
		if err == nil {
			isUtility := strings.ToLower(category) == "utility"
			isPaused := strings.ToLower(status) == "paused"
			return isUtility, isPaused
		}
		
		// Se falhar a API, verificar na lista local como fallback
		logger.Printf("Erro na API, verificando lista local")
	}
	
	// Lista local de templates verificados
	for _, template := range templatesUtilidade {
		if template == templateName {
			return true, false // Assume que templates locais não estão pausados
		}
	}
	
	return false, false
}

// Obtém um template de utilidade confirmado e não pausado
func getConfirmedUtilityTemplate() string {
	// Tenta todos os templates na ordem, começando do próximo após o atual
	startIndex := templateIndex
	for i := 0; i < len(templatesUtilidade); i++ {
		// Calcula o índice circular
		nextIndex := (startIndex + i) % len(templatesUtilidade)
		template := templatesUtilidade[nextIndex]
		
		// Pula o template atual se for o primeiro
		if i == 0 && template == templateToUse {
			continue
		}
		
		isUtility, isPaused := isUtilityTemplate(template)
		if isUtility && !isPaused {
			templateIndex = nextIndex  // Atualiza o índice para a próxima tentativa
			return template
		}
	}
	
	// Nenhum template confirmado
	return ""
}

// Ajusta a taxa de envio com base em feedback
func adjustRate(isRateLimit bool) {
	rateMutex.Lock()
	defer rateMutex.Unlock()
	
	currentRate := atomic.LoadInt32(&taxaEnvio)
	
	if isRateLimit {
		// Ocorreu rate limit, reduzir a taxa em 5 msgs/seg (mínimo 10)
		newRate := int32(math.Max(10, float64(currentRate-5)))
		atomic.StoreInt32(&taxaEnvio, newRate)
		ultimoRateLimit = time.Now()
		logTerminal("🚨 Rate limit detectado! Taxa reduzida para %d msgs/seg", newRate)
	} else if time.Since(ultimoRateLimit) > intervaloEstabilidade {
		// Sem rate limit por 10 segundos, aumentar gradualmente (máximo 50)
		if currentRate < 50 {
			newRate := int32(math.Min(50, float64(currentRate+2)))
			atomic.StoreInt32(&taxaEnvio, newRate)
			logTerminal("✅ Estável por %s. Taxa aumentada para %d msgs/seg", 
				intervaloEstabilidade.String(), newRate)
		}
	}
}

func sendMessage(msg Message, templateName string) bool {
	// SEMPRE marcar como enviado independentemente do resultado
	defer markAsSent(msg.NumeroPedido)
	
	// Verificar se o template é de utilidade e não está pausado
	isUtility, isPaused := isUtilityTemplate(templateName)
	if !isUtility || isPaused {
		logger.Printf("Template %s não é utility ou está pausado. Buscando alternativa.", templateName)
		templateName = getConfirmedUtilityTemplate()
		
		if templateName == "" {
			logger.Printf("Nenhum template utility disponível encontrado. Cancelando envio.")
			atomic.AddInt64(&enviosFalhados, 1)
			updateStats()
			return false
		}
	}
	
	// Determinar a linguagem correta para o template
	language := "pt_BR" // Padrão
	if lang, exists := templateLanguages[templateName]; exists {
		language = lang
	}

	message_template_1 := `Ou deveria...  Quase lá! Só mais um passo e sua pele renovada está garantida!`
	message_template_2 := `Compre 1 Colágeno Verisol + 1 Lip Balm por apenas R$99,90 e leve OUTRO POTE para potencializar seu tratamento!💖  *Oferta imbatível: 3 produtos de qualidade premium com **desconto exclusivo* para você cuidar da sua pele de dentro para fora.`
	
	// Preparar payload
	payload := map[string]interface{}{
		"messaging_product": "whatsapp",
		"to":                msg.Celular,
		"type":              "template",
		"template": map[string]interface{}{
			"name": templateName,
			"language": map[string]string{
				"code": language,
			},
			"components": []map[string]interface{}{
				{
					"type": "body",
					"parameters": []map[string]interface{}{
						{"type": "text", "text": message_template_1},
						{"type": "text", "text": message_template_2},
					},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		logger.Printf("Erro ao serializar JSON: %v", err)
		atomic.AddInt64(&enviosFalhados, 1)
		updateStats()
		return false
	}
	
	// Enviar requisição
	url := fmt.Sprintf("https://graph.facebook.com/v22.0/%s/messages", apiPhoneID)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		logger.Printf("Erro ao criar requisição: %v", err)
		atomic.AddInt64(&enviosFalhados, 1)
		updateStats()
		return false
	}
	
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.Printf("Erro ao enviar: %v", err)
		atomic.AddInt64(&enviosFalhados, 1)
		updateStats()
		return false
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var metaResp MetaResponse
	json.Unmarshal(respBody, &metaResp)
	
	atomic.AddInt64(&totalProcessado, 1)
	updateStats()
	
	// Verificar headers de rate limit se disponíveis
	limitHeader := resp.Header.Get("X-WABA-Ratelimit-Limit")
	remainingHeader := resp.Header.Get("X-WABA-Ratelimit-Remaining")
	
	if limitHeader != "" && remainingHeader != "" {
		logger.Printf("Rate info: Limit=%s, Remaining=%s", 
			limitHeader, remainingHeader)
	}
	
	// Processar resposta - Nota: número já está marcado como enviado
	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		atomic.AddInt64(&enviadosComSucesso, 1)
		return true
	} else if resp.StatusCode == 429 {
		// Rate limit detectado
		atomic.AddInt64(&enviosFalhados, 1)
		atomic.AddInt64(&rateLimitAtingido, 1)
		logger.Printf("Erro 429 (Rate Limit): %s", string(respBody))
		
		// Sinalizar ajuste de taxa
		adjustRate(true)
		return false
	} else {
		atomic.AddInt64(&enviosFalhados, 1)
		logger.Printf("Erro %d: %s", resp.StatusCode, string(respBody))
		return false
	}
}

// Worker que respeita o rate limiting
func worker(wg *sync.WaitGroup) {
	defer wg.Done()
	
	for {
		select {
		case <-doneChan:
			return
		case msg, ok := <-messageQueue:
			if !ok {
				return
			}
			
			// Adquirir semáforo para controle de taxa
			semaphore <- struct{}{}
			
			// Enviar mensagem
			success := sendMessage(msg, templateToUse)
			
			// Gerenciar taxa com base na estabilidade
			if success {
				go func() {
					time.Sleep(time.Second)
					// Se estável por 10s, considera aumentar a taxa
					adjustRate(false)
				}()
			}
			
			// Calcular delay com base na taxa atual
			rate := atomic.LoadInt32(&taxaEnvio)
			delay := time.Duration(1000/rate) * time.Millisecond
			
			// Liberar semáforo após o delay para manter a taxa
			go func() {
				time.Sleep(delay)
				<-semaphore
			}()
		}
	}
}

func main() {
	defer logFile.Close()
	logTerminal("🚀 Iniciando processo de envio controlado")
	
	// Iniciar monitor de estatísticas
	statsDoneCh := make(chan struct{})
	go statsMonitor(statsDoneCh)
	defer close(statsDoneCh)

	const MAX_WORKERS = 200          // Número máximo de workers
	const BATCH_SIZE = 1000          // Tamanho do lote
	const TOTAL_LIMIT = 10000       // Limite total mudar para 25k depois
	const INITIAL_RATE = 50          // Taxa inicial

	// Inicializar taxa
	atomic.StoreInt32(&taxaEnvio, INITIAL_RATE)
	ultimoRateLimit = time.Now().Add(-time.Hour) // Iniciar como se não houvesse rate limit recente

	// Inicializar canais de controle
	messageQueue = make(chan Message, BATCH_SIZE)
	semaphore = make(chan struct{}, INITIAL_RATE) // Controle de rate limiting
	doneChan = make(chan struct{})

	// Iniciar workers
	var wg sync.WaitGroup
	for i := 0; i < MAX_WORKERS; i++ {
		wg.Add(1)
		go worker(&wg)
	}

	var loteProcessados int
	var totalEnviado int64 = 0

	logTerminal("🚀 Iniciando com taxa de %d mensagens por segundo", INITIAL_RATE)

	for totalEnviado < TOTAL_LIMIT {
		// Verificar template a cada lote
		logTerminal("🔍 Verificando template: %s", templateToUse)
		isUtility, isPaused := isUtilityTemplate(templateToUse)
		
		if isPaused {
			logTerminal("⏸️ Template %s está PAUSADO. Tentando próximo template...", templateToUse)
			// Tenta o próximo template
			templateIndex = (templateIndex + 1) % len(templatesUtilidade)
			templateToUse = templatesUtilidade[templateIndex]
			continue
		}
		
		if !isUtility {
			logTerminal("❌ Template %s não é UTILITY. Tentando próximo template...", templateToUse)
			// Tenta o próximo template
			templateIndex = (templateIndex + 1) % len(templatesUtilidade)
			templateToUse = templatesUtilidade[templateIndex]
			continue
		}
		
		logTerminal("✅ Template %s confirmado como UTILITY e ativo. Iniciando envio...", templateToUse)

		// Calcular tamanho do próximo lote
		restantes := TOTAL_LIMIT - int(totalEnviado)
		tamanhoLote := BATCH_SIZE
		if restantes < BATCH_SIZE {
			tamanhoLote = restantes
		}
		
		messages, err := fetchMessages(tamanhoLote)
		if err != nil {
			logTerminal("❌ Erro ao buscar mensagens: %v", err)
			break
		}

		if len(messages) == 0 {
			logTerminal("📊 Não há mais mensagens para processar")
			break
		}

		logTerminal("📤 Processando lote #%d: %d mensagens", loteProcessados+1, len(messages))
		loteProcessados++
		
		// Enviar mensagens para a fila
		for _, msg := range messages {
			messageQueue <- msg
		}
		
		// Aguardar processamento do lote
		for len(messageQueue) > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		
		totalEnviado += int64(len(messages))
		logTerminal("✅ Lote #%d: %d/%d mensagens enviadas", 
			loteProcessados, totalEnviado, TOTAL_LIMIT)
		
		if totalEnviado >= TOTAL_LIMIT {
			logTerminal("🏁 Limite de %d mensagens atingido", TOTAL_LIMIT)
			break
		}
	}
	
	// Finalizar
	close(messageQueue)
	close(doneChan)
	wg.Wait()
	
	// Estatísticas finais
	updateStats()
	time.Sleep(500 * time.Millisecond)
	
	estatisticasFinais := Stats{
		TotalProcessado:    atomic.LoadInt64(&totalProcessado),
		EnviadosComSucesso: atomic.LoadInt64(&enviadosComSucesso),
		EnviosFalhados:     atomic.LoadInt64(&enviosFalhados),
		RateLimitAtingido:  atomic.LoadInt64(&rateLimitAtingido),
		TaxaAtual:          int(atomic.LoadInt32(&taxaEnvio)),
	}
	
	logTerminal("\n📊 RESULTADOS:")
	logTerminal("• Processado: %d", estatisticasFinais.TotalProcessado)
	logTerminal("• Sucesso: %d", estatisticasFinais.EnviadosComSucesso)
	logTerminal("• Falhas: %d", estatisticasFinais.EnviosFalhados)
	logTerminal("• Rate Limits: %d", estatisticasFinais.RateLimitAtingido)
	logTerminal("• Taxa final: %d msgs/seg", estatisticasFinais.TaxaAtual)
	
	if estatisticasFinais.TotalProcessado > 0 {
		taxaSucesso := float64(estatisticasFinais.EnviadosComSucesso) / float64(estatisticasFinais.TotalProcessado) * 100
		logTerminal("• Taxa de sucesso: %.2f%%", taxaSucesso)
	}
	
	logTerminal("\n✅ Concluído! Detalhes em stats/estatisticas_controlado.txt")
} 