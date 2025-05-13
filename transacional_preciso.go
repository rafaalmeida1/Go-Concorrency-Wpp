package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	taxaEnvio          int32 = 20 // Taxa inicial: 20 msgs/seg
	ultimoRateLimit    time.Time
	intervaloEstabilidade = 10 * time.Second // Tempo sem erros para aumentar a taxa
	
	statsUpdateCh = make(chan struct{}, 1)
	startTime     time.Time
	
	// Templates
	templatesUtilidade = []string{
		"transacional_falta_estoque_step_13_05_2025"
	}
	
	templatePadrao = "transacional_falta_estoque_step_13_05_2025"
	templateToUse  = templatePadrao  // Template que será efetivamente utilizado
	
	// Mapeamento de templates para suas linguagens
	templateLanguages = map[string]string{
		"transacional_falta_estoque_step_13_05_2025": "pt_BR"
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
	
	// Controle de uso dos templates
	templateUsage = make(map[string]int64)
	templateUsageMutex sync.Mutex
	MAX_TEMPLATE_USAGE = int64(3000) // Máximo de mensagens por template
)

// Função para encontrar o índice de um template
func findTemplateIndex(template string) int {
	for i, t := range templatesUtilidade {
		if t == template {
			return i
		}
	}
	return 0
}

// Função para incrementar o contador de uso do template
func incrementTemplateUsage(templateName string) {
	templateUsageMutex.Lock()
	defer templateUsageMutex.Unlock()
	templateUsage[templateName]++
}

// Função para obter o próximo template disponível
func getNextAvailableTemplate(currentTemplate string) string {
	templateUsageMutex.Lock()
	defer templateUsageMutex.Unlock()

	// Encontrar o índice do template atual
	currentIndex := findTemplateIndex(currentTemplate)
	
	// Verificar se todos os templates atingiram o limite
	allTemplatesUsed := true
	for _, template := range templatesUtilidade {
		if templateUsage[template] < MAX_TEMPLATE_USAGE {
			allTemplatesUsed = false
			break
		}
	}
	
	// Se todos os templates atingiram o limite, reseta o contador do template atual
	if allTemplatesUsed {
		logTerminal("🔄 Todos os templates atingiram o limite. Resetando contador do template %s", currentTemplate)
		templateUsage[currentTemplate] = 0
		return currentTemplate
	}
	
	// Tentar todos os templates a partir do próximo
	for i := 1; i <= len(templatesUtilidade); i++ {
		nextIndex := (currentIndex + i) % len(templatesUtilidade)
		nextTemplate := templatesUtilidade[nextIndex]
		
		// Se o template não atingiu o limite, retorna ele
		if templateUsage[nextTemplate] < MAX_TEMPLATE_USAGE {
			return nextTemplate
		}
	}
	
	// Se chegou aqui, todos os templates atingiram o limite
	// Reseta o contador do primeiro template e retorna ele
	firstTemplate := templatesUtilidade[0]
	templateUsage[firstTemplate] = 0
	return firstTemplate
}

// Função para animar o tempo de espera e permitir mudança de template
func animateWaitWithTemplateChange(duration time.Duration, currentTemplate string) string {
	start := time.Now()
	end := start.Add(duration)
	
	// Caracteres para a animação
	spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	spinnerIndex := 0
	
	// Canal para receber a resposta do usuário
	templateChan := make(chan string, 1)
	
	// Variável para armazenar o template selecionado
	selectedTemplate := currentTemplate
	templateSelected := false
	
	// Goroutine para perguntar sobre mudança de template
	go func() {
		fmt.Println("\n📋 Deseja mudar o template atual?")
		fmt.Println("Templates disponíveis:")
		for i, template := range templatesUtilidade {
			usage := templateUsage[template]
			fmt.Printf("%d. %s (usado: %d/%d)\n", i+1, template, usage, MAX_TEMPLATE_USAGE)
		}
		fmt.Printf("\nTemplate atual: %s (usado: %d/%d)\n", 
			currentTemplate, templateUsage[currentTemplate], MAX_TEMPLATE_USAGE)
		fmt.Print("\nEscolha o número do novo template (ou 0 para manter o atual): ")
		
		var choice int
		_, err := fmt.Scan(&choice)
		if err != nil {
			fmt.Println("❌ Erro ao ler a escolha. Mantendo template atual.")
			templateChan <- currentTemplate
			return
		}
		
		if choice > 0 && choice <= len(templatesUtilidade) {
			newTemplate := templatesUtilidade[choice-1]
			templateChan <- newTemplate
		} else {
			templateChan <- currentTemplate
		}
	}()
	
	// Loop principal de animação
	for time.Now().Before(end) {
		remaining := end.Sub(time.Now())
		spinnerIndex = (spinnerIndex + 1) % len(spinner)
		
		// Limpar a linha atual
		fmt.Print("\r")
		
		// Mostrar o spinner e o tempo restante
		fmt.Printf("%s Aguardando: %v", spinner[spinnerIndex], remaining.Round(time.Second))
		
		// Verificar se há resposta do usuário
		select {
		case newTemplate := <-templateChan:
			if !templateSelected {
				templateSelected = true
				selectedTemplate = newTemplate
				templateIndex = findTemplateIndex(newTemplate)
				if newTemplate != currentTemplate {
					fmt.Print("\r")
					fmt.Printf("✅ Template alterado para: %s\n", newTemplate)
				} else {
					fmt.Print("\r")
					fmt.Printf("✅ Template mantido como: %s\n", currentTemplate)
				}
			}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	
	// Limpar a linha final
	fmt.Print("\r")
	fmt.Print("                    \r") // Limpar a linha
	
	// Se ainda não houve seleção de template, verificar se há resposta pendente
	if !templateSelected {
		select {
		case newTemplate := <-templateChan:
			selectedTemplate = newTemplate
			templateIndex = findTemplateIndex(newTemplate)
		default:
			// Mantém o template atual
		}
	}
	
	return selectedTemplate
}

// Função para selecionar o template inicial
func selectInitialTemplate() string {
	fmt.Println("\n📋 Templates disponíveis:")
	for i, template := range templatesUtilidade {
		fmt.Printf("%d. %s\n", i+1, template)
	}
	
	var choice int
	for {
		fmt.Print("\nEscolha o número do template inicial (1-", len(templatesUtilidade), "): ")
		_, err := fmt.Scan(&choice)
		if err != nil {
			fmt.Println("❌ Entrada inválida. Por favor, digite um número.")
			continue
		}
		
		if choice < 1 || choice > len(templatesUtilidade) {
			fmt.Printf("❌ Por favor, escolha um número entre 1 e %d.\n", len(templatesUtilidade))
			continue
		}
		
		break
	}
	
	selectedTemplate := templatesUtilidade[choice-1]
	fmt.Printf("\n✅ Template selecionado: %s\n\n", selectedTemplate)
	return selectedTemplate
}

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
	// rows, err := db.Query(`SELECT pedidos_numeroPedido, celular_formatado, pedidos_cliente_nome, pedidos_dataEmissao 
	// FROM pedidos_envio_em_massa 
	// WHERE (enviada = 0 OR enviada IS NULL) 
	// AND pedidos_dataEmissao < '2023-01-01 00:00:00' LIMIT ?`, limit)
	
	rows, err := db.Query(`SELECT pedidos_numeroPedido, celular_formatado, pedidos_cliente_nome, pedidos_dataEmissao 
	FROM pedidos_envio_em_massa 
	WHERE (enviada = 0 OR enviada IS NULL) 
	AND pedidos_dataEmissao >= '2023-01-01 00:00:00' AND pedidos_dataEmissao <= '2025-01-01 00:00:00' LIMIT ?`, limit)

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
			logger.Printf("Número %s já processado anteriormente, marcando pedido %s como enviado", 
				msg.Celular, msg.NumeroPedido)
		}
	}
	return messages, nil
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

// Verifica se um template é de utilidade e não está pausado
func isUtilityTemplate(templateName string) (bool, bool) {
	// Verificação na API
	if usarVerificacaoAPI {
		category, status, err := checkTemplateCategory(templateName)
		if err == nil {
			isUtility := strings.ToLower(category) == "utility"
			isPaused := strings.ToLower(status) == "paused"
			logTerminal("📋 Informações do template '%s':\n   - Categoria: %s\n   - Status: %s", templateName, category, status)
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

func sendMessage(msg Message, templateName string) bool {
	// Incrementar o contador de uso do template
	incrementTemplateUsage(templateName)
	
	// Determinar a linguagem correta para o template
	language := "pt_BR" // Padrão
	if lang, exists := templateLanguages[templateName]; exists {
		language = lang
	}
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
						{"type": "text", "text": msg.Nome},
						{"type": "text", "text": msg.NumeroPedido},
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
			sendMessage(msg, templateToUse)
			
			// Calcular delay com base na taxa fixa
			delay := time.Duration(1000/20) * time.Millisecond // Taxa fixa de 20 msgs/seg
			
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
	
	// Selecionar template inicial
	templateToUse = selectInitialTemplate()
	
	// Iniciar monitor de estatísticas
	statsDoneCh := make(chan struct{})
	go statsMonitor(statsDoneCh)
	defer close(statsDoneCh)

	const MAX_WORKERS = 200          // Número máximo de workers
	const BATCH_SIZE = 1000          // Tamanho do lote
	const TOTAL_LIMIT = 100000       // Limite total de 60k pedidos
	const INITIAL_RATE = 20          // Taxa inicial
	const BATCH_INTERVAL = 5 * time.Minute // Intervalo entre lotes

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
	lastBatchTime := time.Now()

	logTerminal("🚀 Iniciando com taxa de %d mensagens por segundo", INITIAL_RATE)
	logTerminal("⏱️ Processando lotes de %d mensagens a cada %v", BATCH_SIZE, BATCH_INTERVAL)

	for totalEnviado < TOTAL_LIMIT {
		// Verificar se já passou o intervalo mínimo entre lotes
		// Só espera se não for o primeiro lote
		if loteProcessados > 0 {
			timeSinceLastBatch := time.Since(lastBatchTime)
			if timeSinceLastBatch < BATCH_INTERVAL {
				waitTime := BATCH_INTERVAL - timeSinceLastBatch
				logTerminal("⏳ Aguardando %v para o próximo lote...", waitTime.Round(time.Second))
				// Usar a nova função que permite mudança de template durante a espera
				templateToUse = animateWaitWithTemplateChange(waitTime, templateToUse)
			}
		}

		// Verificar template a cada lote
		logTerminal("🔍 Verificando template: %s", templateToUse)
		isUtility, isPaused := isUtilityTemplate(templateToUse)
		
		if isPaused {
			logTerminal("⏸️ Template %s está PAUSADO. Tentando próximo template...", templateToUse)
			// Tenta o próximo template
			templateToUse = getNextAvailableTemplate(templateToUse)
			continue
		}
		
		if !isUtility {
			logTerminal("❌ Template %s não é UTILITY. Tentando próximo template...", templateToUse)
			// Tenta o próximo template
			templateToUse = getNextAvailableTemplate(templateToUse)
			continue
		}
		
		// Verificar se o template atual atingiu o limite de uso
		if templateUsage[templateToUse] >= MAX_TEMPLATE_USAGE {
			logTerminal("📊 Template %s atingiu o limite de %d mensagens. Mudando para o próximo...", 
				templateToUse, MAX_TEMPLATE_USAGE)
			templateToUse = getNextAvailableTemplate(templateToUse)
			continue
		}
		
		logTerminal("✅ Template %s confirmado como UTILITY e ativo. Iniciando envio...", templateToUse)

		// Processar o lote atual
		loteAtual := BATCH_SIZE
		if totalEnviado+int64(loteAtual) > TOTAL_LIMIT {
			loteAtual = int(TOTAL_LIMIT - totalEnviado)
		}
		
		messages, err := fetchMessages(loteAtual)
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
		
		// Registrar tempo do início do lote
		lastBatchTime = time.Now()
		
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