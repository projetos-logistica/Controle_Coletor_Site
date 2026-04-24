📦 Controle Coletor - Site

Este projeto é uma aplicação web desenvolvida com Streamlit para controle de coletores.

🚀 Atualização de usuários

Os usuários são controlados através do arquivo:

usuarios.xlsx
🔄 Passo a passo para atualizar usuários no site

Sempre que houver inclusão ou alteração de usuários:

Atualize o arquivo usuarios.xlsx
No Git, execute:
git add usuarios.xlsx
git commit -m "Atualiza planilha de usuários"
git push origin main
Após o push, acesse o Streamlit Cloud:

👉 https://share.streamlit.io/

Localize o projeto:
controle_coletor_site

Arquivo principal:

site_coletor.py (branch main)
Clique em Reboot para atualizar a aplicação
⚠️ Observações importantes
O site não atualiza automaticamente após o push
O reboot no Streamlit é obrigatório para refletir mudanças no usuarios.xlsx
Evite conflitos no arquivo Excel (não editar simultaneamente)