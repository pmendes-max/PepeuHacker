# Conector do pepeuhacker para o Claude

Deixa o Claude ler o dashboard de vendas da Tractian (pepeuhacker) sozinho:
forecast, pipeline, realizado, por AE, líder ou período. Só leitura, com os
mesmos números da tela, e com a sua conta Google: quem não é @tractian.com não
entra.

## O que precisa

- Mac com o Claude Desktop instalado
- Conta Google @tractian.com
- `gcloud` (Google Cloud CLI). Se não tiver: `brew install --cask google-cloud-sdk`

## Instalar (uns 2 minutos)

1. Descompacte o zip (fica em `~/Downloads/pepeuhacker-conector`).
2. No Terminal:

   ```bash
   bash ~/Downloads/pepeuhacker-conector/instalar.sh
   ```

   Se for o seu primeiro login no gcloud, ele abre o navegador: entre com a
   conta @tractian.com e aceite.
3. Feche o Claude Desktop com Cmd+Q e abra de novo.

Para testar, pergunte numa conversa: *"no pepeuhacker, qual o forecast do
trimestre por AE?"*. A primeira pergunta depois de um tempo parado leva uns 15
segundos; as seguintes são rápidas.

## O que dá pra pedir

- O forecast, o pipeline e o realizado de qualquer tela, por AE, líder ou período
- Os deals por trás de cada número
- Uma URL copiada do dashboard ("me explica essa tela: <url>")
- De quando é o dado: toda resposta traz a data da última atualização

## Se der erro

- **"acesso recusado (403)"**: o login não é @tractian.com. Rode
  `gcloud auth application-default login` com a conta certa e reinicie o Claude.
- **O conector não aparece no Claude**: confira se fechou com Cmd+Q, e não só a
  janela.
- **Outro erro**: rode o `instalar.sh` de novo. Se continuar, mande o arquivo
  `~/.pepeuhacker-conector/ultimo_teste.log` para o Pedro Mendes.

Para desinstalar, apague a pasta `~/.pepeuhacker-conector` e o bloco
`"pepeuhacker"` em
`~/Library/Application Support/Claude/claude_desktop_config.json`.
