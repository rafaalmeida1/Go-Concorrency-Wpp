package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// Estrutura para receber informações de templates da API do Meta
type TemplateInfo struct {
	Data []struct {
		ID         string `json:"id"`
		Name       string `json:"name"`
		Category   string `json:"category"`
		Status     string `json:"status"`
	} `json:"data"`
}

func main() {
	// Carregar variáveis de ambiente
	err := godotenv.Load()
	if err != nil {
		fmt.Printf("Erro ao carregar arquivo .env: %v\n", err)
		return
	}

	// Obter tokens de acesso
	apiToken := os.Getenv("META_ACCESS_TOKEN")
	wabaID := os.Getenv("META_WABA_ID") // ID da conta de negócios do WhatsApp

	if apiToken == "" || wabaID == "" {
		fmt.Println("❌ Erro: META_ACCESS_TOKEN ou META_WABA_ID não configurados no arquivo .env")
		fmt.Println("Adicione META_WABA_ID=seu_waba_id ao arquivo .env")
		return
	}

	// Verificar se o nome do template foi fornecido como argumento
	args := os.Args
	if len(args) < 2 {
		fmt.Println("❌ Uso: go run verificar_template.go <nome_do_template>")
		return
	}

	templateName := args[1]
	fmt.Printf("🔍 Verificando template: %s\n", templateName)

	// Construir URL para consulta de templates (usando o WABA ID, não o Phone ID)
	url := fmt.Sprintf("https://graph.facebook.com/v22.0/%s/message_templates?access_token=%s", wabaID, apiToken)
	fmt.Printf("📡 Consultando API: %s\n", url)

	// Fazer requisição para a API
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("❌ Erro ao consultar API: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// Verificar código de status
	if resp.StatusCode != 200 {
		fmt.Printf("❌ API retornou código %d\n", resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Resposta da API: %s\n", string(body))
		return
	}

	// Ler corpo da resposta
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("❌ Erro ao ler resposta da API: %v\n", err)
		return
	}

	// Imprimir a resposta completa para debug
	fmt.Printf("📄 Resposta completa da API:\n%s\n\n", string(body))

	// Decodificar resposta JSON
	var templateInfo TemplateInfo
	if err := json.Unmarshal(body, &templateInfo); err != nil {
		fmt.Printf("❌ Erro ao decodificar resposta JSON: %v\n", err)
		return
	}

	// Procurar o template pelo nome
	templateEncontrado := false
	for _, template := range templateInfo.Data {
		if template.Name == templateName {
			templateEncontrado = true
			category := template.Category
			isUtility := strings.ToLower(category) == "utility"

			fmt.Printf("\n📋 Informações do template '%s':\n", templateName)
			fmt.Printf("   - ID: %s\n", template.ID)
			fmt.Printf("   - Categoria: %s\n", category)
			fmt.Printf("   - Status: %s\n", template.Status)
			
			if isUtility {
				fmt.Printf("\n✅ O template '%s' é do tipo UTILITY\n", templateName)
			} else {
				fmt.Printf("\n❌ O template '%s' NÃO é do tipo UTILITY (é do tipo %s)\n", templateName, category)
			}
			break
		}
	}

	if !templateEncontrado {
		fmt.Printf("\n❌ Template '%s' não encontrado! Verifique o nome e tente novamente.\n", templateName)
		fmt.Println("\nTemplates disponíveis:")
		for _, template := range templateInfo.Data {
			fmt.Printf("- %s (Categoria: %s)\n", template.Name, template.Category)
		}
	}
} 