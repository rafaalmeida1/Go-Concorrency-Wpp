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
	"sync"
	"time"
	"strings"
	"sync/atomic"

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
	
	statsUpdateCh = make(chan struct{}, 1)
	startTime     time.Time
	
	// Templates
	templatesUtilidade = []string{
		"cashback_disponivel",
		"notificacao_pedido",
		"atualizacao_entrega",
		"confirmacao_agendamento",
		"alerta_seguranca",
	}
	
	templatePadrao = "cashback_disponivel"
	templateToUse  = templatePadrao  // Template que será efetivamente utilizado
	
	// Mapeamento de templates para suas linguagens
	templateLanguages = map[string]string{
		"cashback_disponivel":    "pt_BR",
		"notificacao_pedido":     "pt_BR",
		"atualizacao_entrega":    "pt_BR",
		"confirmacao_agendamento": "pt_BR",
		"alerta_seguranca":       "pt_BR",
		// Adicione outros templates com linguagens diferentes aqui
		// "template_exemplo": "en",
	}
	
	templateCategoryCache   = make(map[string]string)
	templateCacheMutex      = sync.Mutex{}
	usarVerificacaoAPI      = true
	terminalLogger          *log.Logger
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
	logPath := filepath.Join(logDir, fmt.Sprintf("envio_%s.log", timestamp))
	
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
	AND pedidos_dataEmissao >= '2025-05-09 09:30:00' LIMIT ?`, limit)
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
		messages = append(messages, msg)
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
		IniciadoEm:         startTime,
		UltimaAtualizacao:  time.Now(),
		TempoDecorrido:     time.Since(startTime).Round(time.Second).String(),
	}
	
	statsDir := "stats"
	if _, err := os.Stat(statsDir); os.IsNotExist(err) {
		os.Mkdir(statsDir, 0755)
	}
	
	var sb strings.Builder
	sb.WriteString("=== ESTATÍSTICAS DE ENVIO ===\n")
	sb.WriteString(fmt.Sprintf("Iniciado em: %s\n", stats.IniciadoEm.Format("02/01/2006 15:04:05")))
	sb.WriteString(fmt.Sprintf("Última atualização: %s\n", stats.UltimaAtualizacao.Format("02/01/2006 15:04:05")))
	sb.WriteString(fmt.Sprintf("Tempo decorrido: %s\n", stats.TempoDecorrido))
	sb.WriteString(fmt.Sprintf("Total processado: %d\n", stats.TotalProcessado))
	sb.WriteString(fmt.Sprintf("Enviados com sucesso: %d\n", stats.EnviadosComSucesso))
	sb.WriteString(fmt.Sprintf("Envios falhados: %d\n", stats.EnviosFalhados))
	
	if stats.TotalProcessado > 0 {
		taxaSucesso := float64(stats.EnviadosComSucesso) / float64(stats.TotalProcessado) * 100
		sb.WriteString(fmt.Sprintf("Taxa de sucesso: %.2f%%\n", taxaSucesso))
	}
	
	jsonStats, _ := json.MarshalIndent(stats, "", "  ")
	
	statsPath := filepath.Join(statsDir, "estatisticas.txt")
	os.WriteFile(statsPath, []byte(sb.String()), 0644)
	
	jsonStatsPath := filepath.Join(statsDir, "estatisticas.json")
	os.WriteFile(jsonStatsPath, jsonStats, 0644)
}

// Verifica na API do Meta se o template é de utilidade
func checkTemplateCategory(templateName string) (string, error) {
	templateCacheMutex.Lock()
	if category, exists := templateCategoryCache[templateName]; exists {
		templateCacheMutex.Unlock()
		return category, nil
	}
	templateCacheMutex.Unlock()

	url := fmt.Sprintf("https://graph.facebook.com/v22.0/%s/message_templates?access_token=%s", wabaID, apiToken)
	
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("erro ao consultar API: %v", err)
	}
	defer resp.Body.Close()
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("erro ao ler resposta: %v", err)
	}
	
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("API retornou código %d", resp.StatusCode)
	}
	
	var templateInfo TemplateInfo
	if err := json.Unmarshal(body, &templateInfo); err != nil {
		return "", fmt.Errorf("erro ao decodificar JSON: %v", err)
	}
	
	for _, template := range templateInfo.Data {
		if template.Name == templateName {
			category := template.Category
			
			templateCacheMutex.Lock()
			templateCategoryCache[templateName] = category
			templateCacheMutex.Unlock()
			
			return category, nil
		}
	}
	
	return "unknown", fmt.Errorf("template não encontrado")
}

// Verifica se um template é de utilidade
func isUtilityTemplate(templateName string) bool {
	// Verificação na API
	if usarVerificacaoAPI {
		category, err := checkTemplateCategory(templateName)
		if err == nil {
			isUtility := strings.ToLower(category) == "utility"
			return isUtility
		}
		
		// Se falhar a API, verificar na lista local como fallback
		logger.Printf("Erro na API, verificando lista local")
	}
	
	// Lista local de templates verificados
	for _, template := range templatesUtilidade {
		if template == templateName {
			return true
		}
	}
	
	return false
}

// Obtém um template de utilidade confirmado
func getConfirmedUtilityTemplate() string {
	// Tenta primeiro o template padrão
	if isUtilityTemplate(templatePadrao) {
		return templatePadrao
	}
	
	// Tenta outros templates
	for _, template := range templatesUtilidade {
		if template != templatePadrao && isUtilityTemplate(template) {
			return template
		}
	}
	
	// Nenhum template confirmado
	return ""
}

func sendMessage(msg Message, templateName string) {
	// Verificar se o template é de utilidade
	if !isUtilityTemplate(templateName) {
		logger.Printf("Template %s não é utility. Buscando alternativa.", templateName)
		templateName = getConfirmedUtilityTemplate()
		
		if templateName == "" {
			logger.Printf("Nenhum template utility encontrado. Cancelando envio.")
			atomic.AddInt64(&enviosFalhados, 1)
			updateStats()
			return
		}
	}
	
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
		return
	}
	
	// Enviar requisição
	url := fmt.Sprintf("https://graph.facebook.com/v22.0/%s/messages", apiPhoneID)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		logger.Printf("Erro ao criar requisição: %v", err)
		return
	}
	
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.Printf("Erro ao enviar: %v", err)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var metaResp MetaResponse
	json.Unmarshal(respBody, &metaResp)
	
	atomic.AddInt64(&totalProcessado, 1)
	updateStats()
	
	// Processar resposta
	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		atomic.AddInt64(&enviadosComSucesso, 1)
		markAsSent(msg.NumeroPedido)
	} else {
		atomic.AddInt64(&enviosFalhados, 1)
		logger.Printf("Erro %d: %s", resp.StatusCode, string(respBody))
	}
	
	updateStats()
}

func worker(jobs <-chan Message, templateToUse string, wg *sync.WaitGroup) {
	for msg := range jobs {
		sendMessage(msg, templateToUse)
		wg.Done()
	}
}

func main() {
	defer logFile.Close()
	logTerminal("🚀 Iniciando processo de envio em massa")
	
	// Iniciar monitor de estatísticas
	statsDoneCh := make(chan struct{})
	go statsMonitor(statsDoneCh)
	defer close(statsDoneCh)
	
	// Verificar template inicial
	logTerminal("🔍 Verificando template inicial: %s", templateToUse)
	
	// Verificar se o template é utility
	isUtility := false
	
	// Tentar verificar na API primeiro
	category, err := checkTemplateCategory(templateToUse)
	if err == nil && strings.ToLower(category) == "utility" {
		isUtility = true
		logTerminal("✅ Template %s confirmado como UTILITY", templateToUse)
	} else {
		if err != nil {
			logTerminal("⚠️ Erro na verificação na API: %v", err)
		} else {
			logTerminal("⚠️ Template %s tem categoria %s (não é UTILITY)", templateToUse, category)
		}
		
		// Verificar por método alternativo
		isUtility = isUtilityTemplate(templateToUse)
	}
	
	// Se o template inicial não for utility, tentar encontrar outro
	if !isUtility {
		logTerminal("🔄 Buscando template alternativo de utilidade...")
		alternativeTemplate := getConfirmedUtilityTemplate()
		
		if alternativeTemplate == "" {
			logTerminal("❌ Não foi encontrado nenhum template de utilidade. Operação cancelada.")
			return
		}
		
		// Usar o template alternativo encontrado
		templateToUse = alternativeTemplate
		logTerminal("✅ Usando template alternativo: %s", templateToUse)
	}
	
	logTerminal("✅ Template confirmado. Iniciando envio...")

	const MAX_CONCURRENCY = 100
	const BATCH_SIZE = 250
	const TOTAL_LIMIT = 1000

	var loteProcessados int
	var totalEnviado int64 = 0
	
	for totalEnviado < TOTAL_LIMIT {
		// Calcular tamanho do próximo lote
		restantes := TOTAL_LIMIT - int(totalEnviado)
		tamanhoLote := BATCH_SIZE
		if restantes < BATCH_SIZE {
			tamanhoLote = restantes
		}
		
		messages, err := fetchMessages(tamanhoLote)
		if err != nil {
			logTerminal("❌ Erro ao buscar mensagens: %v", err)
			return
		}

		if len(messages) == 0 {
			logTerminal("📊 Não há mais mensagens para processar")
			break
		}

		logTerminal("📤 Processando lote #%d: %d mensagens", loteProcessados+1, len(messages))
		loteProcessados++

		// Processar lote com workers
		jobs := make(chan Message, len(messages))
		var wg sync.WaitGroup

		for i := 0; i < MAX_CONCURRENCY; i++ {
			go worker(jobs, templateToUse, &wg)
		}

		for _, msg := range messages {
			wg.Add(1)
			jobs <- msg
		}

		close(jobs)
		wg.Wait()
		
		totalEnviado += int64(len(messages))
		logTerminal("✅ Lote #%d: %d/%d mensagens enviadas", 
			loteProcessados, totalEnviado, TOTAL_LIMIT)
		
		if totalEnviado >= TOTAL_LIMIT {
			logTerminal("🏁 Limite de %d mensagens atingido", TOTAL_LIMIT)
			break
		}
	}

	// Estatísticas finais
	updateStats()
	time.Sleep(500 * time.Millisecond)
	
	estatisticasFinais := Stats{
		TotalProcessado:    atomic.LoadInt64(&totalProcessado),
		EnviadosComSucesso: atomic.LoadInt64(&enviadosComSucesso),
		EnviosFalhados:     atomic.LoadInt64(&enviosFalhados),
	}
	
	logTerminal("\n📊 RESULTADOS:")
	logTerminal("• Processado: %d", estatisticasFinais.TotalProcessado)
	logTerminal("• Sucesso: %d", estatisticasFinais.EnviadosComSucesso)
	logTerminal("• Falhas: %d", estatisticasFinais.EnviosFalhados)
	
	if estatisticasFinais.TotalProcessado > 0 {
		taxaSucesso := float64(estatisticasFinais.EnviadosComSucesso) / float64(estatisticasFinais.TotalProcessado) * 100
		logTerminal("• Taxa de sucesso: %.2f%%", taxaSucesso)
	}
	
	logTerminal("\n✅ Concluído! Detalhes em stats/estatisticas.txt")
}
