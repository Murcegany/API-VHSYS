using System;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Data.SqlTypes;

class Program
{
    static async Task Main(string[] args)
    {
        string connectionString = "Data Source=x;User ID=y;Password=123;Initial Catalog=z";

        try
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                if (connection.State != ConnectionState.Open)
                {
                    throw new Exception("Erro na conexão com o banco de dados.");
                }

                string apiAddress = "https://api.vhsys.com/v2/clientes";
                string accessToken = "token";
                string secretAccessToken = "secret";
                bool buscarPorDataCadastro = true; // Variável para controlar o tipo de busca inicial

                int pageSize = 250;
                int offset = 0;
                // pedidos até 1 dia de atraso
                //DateTime dataLimite = DateTime.Now.AddDays(-1);
                //pedidos hoje 
                DateTime dataLimite = DateTime.Today;
                DateTime dataUltimaAtualizacaoC1 = ObterDataUltimaAtualizacaoC1(connection, "ultima_atualizacao_C1");

                while (true)
                {

                    using (HttpClient httpClient = new HttpClient())
                    {
                        httpClient.DefaultRequestHeaders.Add("Access-Token", accessToken);
                        httpClient.DefaultRequestHeaders.Add("Secret-Access-Token", secretAccessToken);
                        string campoData = buscarPorDataCadastro ? "data_cad_cliente" : "data_mod_cliente";

                        HttpResponseMessage response = await httpClient.GetAsync($"https://api.vhsys.com/v2/clientes?offset={offset}&limit={pageSize}&order={campoData}&sort=Desc");

                        if (response.IsSuccessStatusCode)
                        {
                            string jsonResponse = await response.Content.ReadAsStringAsync();
                            JObject data = JObject.Parse(jsonResponse);

                            if (data["data"] is JArray dataArray)
                            {
                                foreach (var cliente in dataArray)
                                {
                                    try
                                    {
                                        
                                    DateTime? dataModCliente = GetDateTimeOrNull(cliente, "data_mod_cliente");
                                    DateTime? dataCadCliente = GetDateTimeOrNull(cliente, "data_cad_cliente");
                                    
                                    if (dataCadCliente >= dataLimite && buscarPorDataCadastro)
                                        {
                                            using (SqlCommand command = connection.CreateCommand())
                                            {

                                                string mergeC1Query = @"
                                                    MERGE INTO C1 AS target
                                                    USING (
                                                        SELECT 
                                                            COALESCE(NULLIF(@id_cliente, ''), '0') AS id_cliente, 
                                                            COALESCE(NULLIF(@id_registro, ''), '0') AS id_registro, 
                                                            COALESCE(NULLIF(@tipo_pessoa, ''), 'null') AS tipo_pessoa, 
                                                            COALESCE(NULLIF(@tipo_cadastro, ''), 'null') AS tipo_cadastro, 
                                                            COALESCE(NULLIF(@cnpj_cliente, ''), 'null') AS cnpj_cliente,
                                                            COALESCE(NULLIF(@rg_cliente, ''), 'null') AS rg_cliente,
                                                            COALESCE(NULLIF(@data_emissao_rg_cliente, ''), 'null') AS data_emissao_rg_cliente,
                                                            COALESCE(NULLIF(@orgao_expedidor_rg_cliente, ''), 'null') AS orgao_expedidor_rg_cliente,
                                                            COALESCE(NULLIF(@passaporte_cliente, ''), 'null') AS passaporte_cliente,
                                                            COALESCE(NULLIF(@estrangeiro_cliente, ''), 'null') AS estrangeiro_cliente,
                                                            COALESCE(NULLIF(@razao_cliente, ''), 'null') AS razao_cliente,
                                                            COALESCE(NULLIF(@fantasia_cliente, ''), 'null') AS fantasia_cliente,
                                                            COALESCE(NULLIF(@endereco_cliente, ''), 'null') AS endereco_cliente,
                                                            COALESCE(NULLIF(@numero_cliente, ''), '0') AS numero_cliente,
                                                            COALESCE(NULLIF(@bairro_cliente, ''), 'null') AS bairro_cliente,
                                                            COALESCE(NULLIF(@complemento_cliente, ''), 'null') AS complemento_cliente,
                                                            COALESCE(NULLIF(@referencia_cliente, ''), 'null') AS referencia_cliente,
                                                            COALESCE(NULLIF(@cep_cliente, ''), 'null') AS cep_cliente,
                                                            COALESCE(NULLIF(@cidade_cliente, ''), 'null') AS cidade_cliente,
                                                            COALESCE(NULLIF(@cidade_cliente_cod, ''), '0') AS cidade_cliente_cod,
                                                            COALESCE(NULLIF(@uf_cliente, ''), 'null') AS uf_cliente,
                                                            COALESCE(NULLIF(@tel_destinatario_cliente, ''), 'null') AS tel_destinatario_cliente,
                                                            COALESCE(NULLIF(@doc_destinatario_cliente, ''), 'null') AS doc_destinatario_cliente,
                                                            COALESCE(NULLIF(@nome_destinatario_cliente, ''), 'null') AS nome_destinatario_cliente,
                                                            COALESCE(NULLIF(@contato_cliente, ''), 'null') AS contato_cliente,
                                                            COALESCE(NULLIF(@fone_cliente, ''), 'null') AS fone_cliente,
                                                            COALESCE(NULLIF(@fone_contato_cliente, ''), 'null') AS fone_contato_cliente,
                                                            COALESCE(NULLIF(@fone_ramal_cliente, ''), 'null') AS fone_ramal_cliente,
                                                            COALESCE(NULLIF(@fax_cliente, ''), 'null') AS fax_cliente,
                                                            COALESCE(NULLIF(@celular_cliente, ''), 'null') AS celular_cliente,
                                                            COALESCE(NULLIF(@email_cliente, ''), 'null') AS email_cliente,
                                                            COALESCE(NULLIF(@email_contato_cliente, ''), 'null') AS email_contato_cliente,
                                                            COALESCE(NULLIF(@celular_contato_cliente, ''), 'null') AS celular_contato_cliente,
                                                            COALESCE(NULLIF(@estado_civil_cliente, ''), 'null') AS estado_civil_cliente,
                                                            COALESCE(NULLIF(@website_cliente, ''), 'null') AS website_cliente,
                                                            COALESCE(NULLIF(@aposentado_cliente, ''), 'null') AS aposentado_cliente,
                                                            COALESCE(NULLIF(@empregador_cliente, ''), 'null') AS empregador_cliente,
                                                            COALESCE(NULLIF(@profissao_cliente, ''), 'null') AS profissao_cliente,
                                                            COALESCE(NULLIF(@genero_cliente, ''), 'null') AS genero_cliente,
                                                            COALESCE(NULLIF(@insc_estadual_cliente, ''), 'null') AS insc_estadual_cliente,
                                                            COALESCE(NULLIF(@insc_municipal_cliente, ''), 'null') AS insc_municipal_cliente,
                                                            COALESCE(NULLIF(@insc_produtor_cliente, ''), 'null') AS insc_produtor_cliente,
                                                            COALESCE(NULLIF(@insc_suframa_cliente, ''), 'null') AS insc_suframa_cliente,
                                                            COALESCE(NULLIF(@nif, ''), 'null') AS nif,
                                                            COALESCE(NULLIF(@situacao_cliente, ''), 'Ativo') AS situacao_cliente,
                                                            COALESCE(NULLIF(@vendedor_cliente, ''), 'null') AS vendedor_cliente,
                                                            CASE WHEN NULLIF(@vendedor_cliente_id, '') = '' THEN NULL ELSE CAST(@vendedor_cliente_id AS INT) END AS vendedor_cliente_id,
                                                            COALESCE(NULLIF(@modalidade_frete, ''), 'null') AS modalidade_frete,
                                                            COALESCE(NULLIF(@id_transportadora, ''), 'null') AS id_transportadora,
                                                            COALESCE(NULLIF(@desc_transportadora, ''), 'null') AS desc_transportadora,
                                                            COALESCE(NULLIF(@observacoes_cliente, ''), 'null') AS observacoes_cliente,
                                                            COALESCE(NULLIF(@listapreco_cliente, ''), '0') AS listapreco_cliente,
                                                            COALESCE(NULLIF(@condicaopag_cliente, ''), 'null') AS condicaopag_cliente,
                                                            COALESCE(NULLIF(@limite_credito, ''), '0') AS limite_credito,
                                                            CASE WHEN NULLIF(@ultrapassar_limite_credito, '') = '' THEN 'false' ELSE @ultrapassar_limite_credito END AS ultrapassar_limite_credito,
                                                            CASE WHEN NULLIF(@consumidor_final, '') = '' THEN 'false' ELSE @consumidor_final END AS consumidor_final,
                                                            CASE WHEN NULLIF(@contribuinte_icms, '') = '' THEN 'false' ELSE @contribuinte_icms END AS contribuinte_icms,
                                                            CASE WHEN NULLIF(@atividade_encerrada_cliente, '') = '' THEN 'false' ELSE @atividade_encerrada_cliente END AS atividade_encerrada_cliente,
                                                            CASE WHEN NULLIF(@data_nasc_cliente, '') = '' THEN GETDATE() ELSE @data_nasc_cliente END AS data_nasc_cliente,
                                                            CASE WHEN NULLIF(@data_cad_cliente, '') = '' THEN GETDATE() ELSE @data_cad_cliente END AS data_cad_cliente,
                                                            CASE WHEN NULLIF(@data_mod_cliente, '') = '' THEN GETDATE() ELSE @data_mod_cliente END AS data_mod_cliente,
                                                            CASE WHEN NULLIF(@lixeira, '') = '' THEN 'Nao' ELSE @lixeira END AS lixeira                                    ) AS source
                                                    ON (target.id_cliente = source.id_cliente)
                                                    WHEN MATCHED THEN
                                                        UPDATE SET
                                                            target.id_registro = source.id_registro,
                                                            target.tipo_pessoa = source.tipo_pessoa,
                                                            target.tipo_cadastro = source.tipo_cadastro,
                                                            target.cnpj_cliente = source.cnpj_cliente,
                                                            target.rg_cliente = source.rg_cliente,
                                                            target.data_emissao_rg_cliente = source.data_emissao_rg_cliente,
                                                            target.orgao_expedidor_rg_cliente = source.orgao_expedidor_rg_cliente,
                                                            target.passaporte_cliente = source.passaporte_cliente,
                                                            target.estrangeiro_cliente = source.estrangeiro_cliente,
                                                            target.razao_cliente = source.razao_cliente,
                                                            target.fantasia_cliente = source.fantasia_cliente,
                                                            target.endereco_cliente = source.endereco_cliente,
                                                            target.numero_cliente = source.numero_cliente,
                                                            target.bairro_cliente = source.bairro_cliente,
                                                            target.complemento_cliente = source.complemento_cliente,
                                                            target.referencia_cliente = source.referencia_cliente,
                                                            target.cep_cliente = source.cep_cliente,
                                                            target.cidade_cliente = source.cidade_cliente,
                                                            target.cidade_cliente_cod = source.cidade_cliente_cod,
                                                            target.uf_cliente = source.uf_cliente,
                                                            target.tel_destinatario_cliente = source.tel_destinatario_cliente,
                                                            target.doc_destinatario_cliente = source.doc_destinatario_cliente,
                                                            target.nome_destinatario_cliente = source.nome_destinatario_cliente,
                                                            target.contato_cliente = source.contato_cliente,
                                                            target.fone_cliente = source.fone_cliente,
                                                            target.fone_contato_cliente = source.fone_contato_cliente,
                                                            target.fone_ramal_cliente = source.fone_ramal_cliente,
                                                            target.fax_cliente = source.fax_cliente,
                                                            target.celular_cliente = source.celular_cliente,
                                                            target.email_cliente = source.email_cliente,
                                                            target.email_contato_cliente = source.email_contato_cliente,
                                                            target.celular_contato_cliente = source.celular_contato_cliente,
                                                            target.estado_civil_cliente = source.estado_civil_cliente,
                                                            target.website_cliente = source.website_cliente,
                                                            target.aposentado_cliente = source.aposentado_cliente,
                                                            target.empregador_cliente = source.empregador_cliente,
                                                            target.profissao_cliente = source.profissao_cliente,
                                                            target.genero_cliente = source.genero_cliente,
                                                            target.insc_estadual_cliente = source.insc_estadual_cliente,
                                                            target.insc_municipal_cliente = source.insc_municipal_cliente,
                                                            target.insc_produtor_cliente = source.insc_produtor_cliente,
                                                            target.insc_suframa_cliente = source.insc_suframa_cliente,
                                                            target.nif = source.nif,
                                                            target.situacao_cliente = source.situacao_cliente,
                                                            target.vendedor_cliente = source.vendedor_cliente,
                                                            target.vendedor_cliente_id = source.vendedor_cliente_id,
                                                            target.modalidade_frete = source.modalidade_frete,
                                                            target.id_transportadora = source.id_transportadora,
                                                            target.desc_transportadora = source.desc_transportadora,
                                                            target.observacoes_cliente = source.observacoes_cliente,
                                                            target.listapreco_cliente = source.listapreco_cliente,
                                                            target.condicaopag_cliente = source.condicaopag_cliente,
                                                            target.limite_credito = source.limite_credito,
                                                            target.ultrapassar_limite_credito = source.ultrapassar_limite_credito,
                                                            target.consumidor_final = source.consumidor_final,
                                                            target.contribuinte_icms = source.contribuinte_icms,
                                                            target.atividade_encerrada_cliente = source.atividade_encerrada_cliente,
                                                            target.data_nasc_cliente = source.data_nasc_cliente,
                                                            target.data_mod_cliente = source.data_mod_cliente,
                                                            target.lixeira = source.lixeira
                                                    WHEN NOT MATCHED THEN
                                                        INSERT (
                                                            id_cliente, id_registro, tipo_pessoa, tipo_cadastro, cnpj_cliente, rg_cliente,
                                                            data_emissao_rg_cliente, orgao_expedidor_rg_cliente, passaporte_cliente, estrangeiro_cliente,
                                                            razao_cliente, fantasia_cliente, endereco_cliente, numero_cliente, bairro_cliente,
                                                            complemento_cliente, referencia_cliente, cep_cliente, cidade_cliente, cidade_cliente_cod,
                                                            uf_cliente, tel_destinatario_cliente, doc_destinatario_cliente, nome_destinatario_cliente,
                                                            contato_cliente, fone_cliente, fone_contato_cliente, fone_ramal_cliente, fax_cliente,
                                                            celular_cliente, email_cliente, email_contato_cliente, celular_contato_cliente,
                                                            estado_civil_cliente, website_cliente, aposentado_cliente, empregador_cliente,
                                                            profissao_cliente, genero_cliente, insc_estadual_cliente, insc_municipal_cliente,
                                                            insc_produtor_cliente, insc_suframa_cliente, nif, situacao_cliente, vendedor_cliente,
                                                            vendedor_cliente_id, modalidade_frete, id_transportadora, desc_transportadora, observacoes_cliente,
                                                            listapreco_cliente, condicaopag_cliente, limite_credito, ultrapassar_limite_credito,
                                                            consumidor_final, contribuinte_icms, atividade_encerrada_cliente, data_nasc_cliente, data_cad_cliente,
                                                            data_mod_cliente, lixeira
                                                        )
                                                        VALUES (
                                                            source.id_cliente, source.id_registro, source.tipo_pessoa, source.tipo_cadastro, source.cnpj_cliente, source.rg_cliente,
                                                            source.data_emissao_rg_cliente, source.orgao_expedidor_rg_cliente, source.passaporte_cliente, source.estrangeiro_cliente,
                                                            source.razao_cliente, source.fantasia_cliente, source.endereco_cliente, source.numero_cliente, source.bairro_cliente,
                                                            source.complemento_cliente, source.referencia_cliente, source.cep_cliente, source.cidade_cliente, source.cidade_cliente_cod,
                                                            source.uf_cliente, source.tel_destinatario_cliente, source.doc_destinatario_cliente, source.nome_destinatario_cliente,
                                                            source.contato_cliente, source.fone_cliente, source.fone_contato_cliente, source.fone_ramal_cliente, source.fax_cliente,
                                                            source.celular_cliente, source.email_cliente, source.email_contato_cliente, source.celular_contato_cliente,
                                                            source.estado_civil_cliente, source.website_cliente, source.aposentado_cliente, source.empregador_cliente,
                                                            source.profissao_cliente, source.genero_cliente, source.insc_estadual_cliente, source.insc_municipal_cliente,
                                                            source.insc_produtor_cliente, source.insc_suframa_cliente, source.nif, source.situacao_cliente, source.vendedor_cliente,
                                                            source.vendedor_cliente_id, source.modalidade_frete, source.id_transportadora, source.desc_transportadora, source.observacoes_cliente,
                                                            source.listapreco_cliente, source.condicaopag_cliente, source.limite_credito, source.ultrapassar_limite_credito,
                                                            source.consumidor_final, source.contribuinte_icms, source.atividade_encerrada_cliente, source.data_nasc_cliente, source.data_cad_cliente,
                                                            source.data_mod_cliente, source.lixeira
                                                        );
                                                ";

                                                // Executar a consulta MERGE na conexão do SQL Server
                                                using (SqlCommand mergeCmd = new SqlCommand(mergeC1Query, connection))
                                                {
                                                    // Defina o valor de @id_cliente com base na condição
                                                    string idClienteValue = cliente["id_cliente"]?.ToString();
                                                    if (string.IsNullOrWhiteSpace(idClienteValue) || !int.TryParse(idClienteValue, out int idCliente))
                                                    {
                                                        idCliente = 0; // Valor padrão para valores inválidos
                                                    }
                                                    mergeCmd.Parameters.AddWithValue("@id_cliente", cliente["id_cliente"].ToString());
                                                    

                                                    // Defina o valor de @id_registro com base na condição
                                                    string idRegistroValue = cliente["id_registro"]?.ToString();
                                                    if (string.IsNullOrWhiteSpace(idRegistroValue) || !int.TryParse(idRegistroValue, out int idRegistro))
                                                    {
                                                        idCliente = 0; // Valor padrão para valores inválidos
                                                    } 
                                                    mergeCmd.Parameters.AddWithValue("@id_registro", cliente["id_registro"].ToString());
                                                    

                                                    mergeCmd.Parameters.AddWithValue("@tipo_pessoa", cliente["tipo_pessoa"].ToString());
                                                    mergeCmd.Parameters.AddWithValue("@tipo_cadastro", cliente["tipo_cadastro"].ToString());
                                                    mergeCmd.Parameters.AddWithValue("@cnpj_cliente", cliente["cnpj_cliente"].ToString());
                                                    mergeCmd.Parameters.AddWithValue("@rg_cliente", cliente["rg_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@data_emissao_rg_cliente", cliente["data_emissao_rg_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@orgao_expedidor_rg_cliente", cliente["orgao_expedidor_rg_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@passaporte_cliente", cliente["passaporte_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@estrangeiro_cliente", cliente["estrangeiro_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@razao_cliente", cliente["razao_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fantasia_cliente", cliente["fantasia_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@endereco_cliente", cliente["endereco_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@numero_cliente", cliente["numero_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@bairro_cliente", cliente["bairro_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@complemento_cliente", cliente["complemento_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@referencia_cliente", cliente["referencia_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@cep_cliente", cliente["cep_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@cidade_cliente", cliente["cidade_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@cidade_cliente_cod", cliente["cidade_cliente_cod"]?.ToString() ?? "0");
                                                    mergeCmd.Parameters.AddWithValue("@uf_cliente", cliente["uf_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@tel_destinatario_cliente", cliente["tel_destinatario_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@doc_destinatario_cliente", cliente["doc_destinatario_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@nome_destinatario_cliente", cliente["nome_destinatario_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@contato_cliente", cliente["contato_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fone_cliente", cliente["fone_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fone_contato_cliente", cliente["fone_contato_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fone_ramal_cliente", cliente["fone_ramal_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fax_cliente", cliente["fax_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@celular_cliente", cliente["celular_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@email_cliente", cliente["email_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@email_contato_cliente", cliente["email_contato_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@celular_contato_cliente", cliente["celular_contato_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@estado_civil_cliente", cliente["estado_civil_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@website_cliente", cliente["website_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@aposentado_cliente", cliente["aposentado_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@empregador_cliente", cliente["empregador_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@profissao_cliente", cliente["profissao_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@genero_cliente", cliente["genero_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@insc_estadual_cliente", cliente["insc_estadual_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@insc_municipal_cliente", cliente["insc_municipal_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@insc_produtor_cliente", cliente["insc_produtor_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@insc_suframa_cliente", cliente["insc_suframa_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@nif", cliente["nif"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@situacao_cliente", cliente["situacao_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@vendedor_cliente", cliente["vendedor_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@vendedor_cliente_id", cliente["vendedor_cliente_id"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@modalidade_frete", cliente["modalidade_frete"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@id_transportadora", cliente["id_transportadora"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@desc_transportadora", cliente["desc_transportadora"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@observacoes_cliente", cliente["observacoes_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@listapreco_cliente", cliente["listapreco_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@condicaopag_cliente", cliente["condicaopag_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@limite_credito", cliente["limite_credito"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@ultrapassar_limite_credito", cliente["ultrapassar_limite_credito"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@consumidor_final", cliente["consumidor_final"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@contribuinte_icms", cliente["contribuinte_icms"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@atividade_encerrada_cliente", cliente["atividade_encerrada_cliente"]?.ToString() ?? "null");

                                                    string dataNascClienteValue = cliente["data_nasc_cliente"]?.ToString();
                                                    if (string.IsNullOrWhiteSpace(dataNascClienteValue) || !DateTime.TryParse(dataNascClienteValue, out DateTime dataNascCliente) || dataNascCliente < SqlDateTime.MinValue.Value || dataNascCliente > SqlDateTime.MaxValue.Value)
                                                    {
                                                        dataNascCliente = SqlDateTime.MinValue.Value; // Valor padrão para datas inválidas
                                                    }
                                                    mergeCmd.Parameters.AddWithValue("@data_nasc_cliente", dataNascCliente);

                                                    mergeCmd.Parameters.AddWithValue("@data_cad_cliente", dataCadCliente);
                                                    mergeCmd.Parameters.AddWithValue("@data_mod_cliente", dataModCliente);
                                                    

                                                    // Defina o valor de @lixeira com base na condição
                                                    mergeCmd.Parameters.AddWithValue("@lixeira", (cliente["lixeira"].ToString().ToLower() == "Nao") ? false : true);
                                    
                                                    // linha utilizada para teste de resposta
                                                    foreach (SqlParameter parameter in mergeCmd.Parameters)
                                                    {
                                                        Console.WriteLine($"{parameter.ParameterName}: {parameter.Value}");
                                                    }

                                                    await mergeCmd.ExecuteNonQueryAsync();
                                                    Console.WriteLine("Cliente inserido/atualizado cadastro com sucesso na tabela C1!");
                                                }
                                            }
                                        }

                                    else if (dataModCliente >= dataLimite && !buscarPorDataCadastro)
                                        {
                                            using (SqlCommand command = connection.CreateCommand())
                                            {

                                                // Construa a consulta MERGE para a tabela P1
                                                string mergeC1Query = @"
                                                    MERGE INTO C1 AS target
                                                    USING (
                                                        SELECT 
                                                            COALESCE(NULLIF(@id_cliente, ''), '0') AS id_cliente, 
                                                            COALESCE(NULLIF(@id_registro, ''), '0') AS id_registro, 
                                                            COALESCE(NULLIF(@tipo_pessoa, ''), 'null') AS tipo_pessoa, 
                                                            COALESCE(NULLIF(@tipo_cadastro, ''), 'null') AS tipo_cadastro, 
                                                            COALESCE(NULLIF(@cnpj_cliente, ''), 'null') AS cnpj_cliente,
                                                            COALESCE(NULLIF(@rg_cliente, ''), 'null') AS rg_cliente,
                                                            COALESCE(NULLIF(@data_emissao_rg_cliente, ''), 'null') AS data_emissao_rg_cliente,
                                                            COALESCE(NULLIF(@orgao_expedidor_rg_cliente, ''), 'null') AS orgao_expedidor_rg_cliente,
                                                            COALESCE(NULLIF(@passaporte_cliente, ''), 'null') AS passaporte_cliente,
                                                            COALESCE(NULLIF(@estrangeiro_cliente, ''), 'null') AS estrangeiro_cliente,
                                                            COALESCE(NULLIF(@razao_cliente, ''), 'null') AS razao_cliente,
                                                            COALESCE(NULLIF(@fantasia_cliente, ''), 'null') AS fantasia_cliente,
                                                            COALESCE(NULLIF(@endereco_cliente, ''), 'null') AS endereco_cliente,
                                                            COALESCE(NULLIF(@numero_cliente, ''), '0') AS numero_cliente,
                                                            COALESCE(NULLIF(@bairro_cliente, ''), 'null') AS bairro_cliente,
                                                            COALESCE(NULLIF(@complemento_cliente, ''), 'null') AS complemento_cliente,
                                                            COALESCE(NULLIF(@referencia_cliente, ''), 'null') AS referencia_cliente,
                                                            COALESCE(NULLIF(@cep_cliente, ''), 'null') AS cep_cliente,
                                                            COALESCE(NULLIF(@cidade_cliente, ''), 'null') AS cidade_cliente,
                                                            COALESCE(NULLIF(@cidade_cliente_cod, ''), '0') AS cidade_cliente_cod,
                                                            COALESCE(NULLIF(@uf_cliente, ''), 'null') AS uf_cliente,
                                                            COALESCE(NULLIF(@tel_destinatario_cliente, ''), 'null') AS tel_destinatario_cliente,
                                                            COALESCE(NULLIF(@doc_destinatario_cliente, ''), 'null') AS doc_destinatario_cliente,
                                                            COALESCE(NULLIF(@nome_destinatario_cliente, ''), 'null') AS nome_destinatario_cliente,
                                                            COALESCE(NULLIF(@contato_cliente, ''), 'null') AS contato_cliente,
                                                            COALESCE(NULLIF(@fone_cliente, ''), 'null') AS fone_cliente,
                                                            COALESCE(NULLIF(@fone_contato_cliente, ''), 'null') AS fone_contato_cliente,
                                                            COALESCE(NULLIF(@fone_ramal_cliente, ''), 'null') AS fone_ramal_cliente,
                                                            COALESCE(NULLIF(@fax_cliente, ''), 'null') AS fax_cliente,
                                                            COALESCE(NULLIF(@celular_cliente, ''), 'null') AS celular_cliente,
                                                            COALESCE(NULLIF(@email_cliente, ''), 'null') AS email_cliente,
                                                            COALESCE(NULLIF(@email_contato_cliente, ''), 'null') AS email_contato_cliente,
                                                            COALESCE(NULLIF(@celular_contato_cliente, ''), 'null') AS celular_contato_cliente,
                                                            COALESCE(NULLIF(@estado_civil_cliente, ''), 'null') AS estado_civil_cliente,
                                                            COALESCE(NULLIF(@website_cliente, ''), 'null') AS website_cliente,
                                                            COALESCE(NULLIF(@aposentado_cliente, ''), 'null') AS aposentado_cliente,
                                                            COALESCE(NULLIF(@empregador_cliente, ''), 'null') AS empregador_cliente,
                                                            COALESCE(NULLIF(@profissao_cliente, ''), 'null') AS profissao_cliente,
                                                            COALESCE(NULLIF(@genero_cliente, ''), 'null') AS genero_cliente,
                                                            COALESCE(NULLIF(@insc_estadual_cliente, ''), 'null') AS insc_estadual_cliente,
                                                            COALESCE(NULLIF(@insc_municipal_cliente, ''), 'null') AS insc_municipal_cliente,
                                                            COALESCE(NULLIF(@insc_produtor_cliente, ''), 'null') AS insc_produtor_cliente,
                                                            COALESCE(NULLIF(@insc_suframa_cliente, ''), 'null') AS insc_suframa_cliente,
                                                            COALESCE(NULLIF(@nif, ''), 'null') AS nif,
                                                            COALESCE(NULLIF(@situacao_cliente, ''), 'Ativo') AS situacao_cliente,
                                                            COALESCE(NULLIF(@vendedor_cliente, ''), 'null') AS vendedor_cliente,
                                                            CASE WHEN NULLIF(@vendedor_cliente_id, '') = '' THEN NULL ELSE CAST(@vendedor_cliente_id AS INT) END AS vendedor_cliente_id,
                                                            COALESCE(NULLIF(@modalidade_frete, ''), 'null') AS modalidade_frete,
                                                            COALESCE(NULLIF(@id_transportadora, ''), 'null') AS id_transportadora,
                                                            COALESCE(NULLIF(@desc_transportadora, ''), 'null') AS desc_transportadora,
                                                            COALESCE(NULLIF(@observacoes_cliente, ''), 'null') AS observacoes_cliente,
                                                            COALESCE(NULLIF(@listapreco_cliente, ''), '0') AS listapreco_cliente,
                                                            COALESCE(NULLIF(@condicaopag_cliente, ''), 'null') AS condicaopag_cliente,
                                                            COALESCE(NULLIF(@limite_credito, ''), '0') AS limite_credito,
                                                            CASE WHEN NULLIF(@ultrapassar_limite_credito, '') = '' THEN 'false' ELSE @ultrapassar_limite_credito END AS ultrapassar_limite_credito,
                                                            CASE WHEN NULLIF(@consumidor_final, '') = '' THEN 'false' ELSE @consumidor_final END AS consumidor_final,
                                                            CASE WHEN NULLIF(@contribuinte_icms, '') = '' THEN 'false' ELSE @contribuinte_icms END AS contribuinte_icms,
                                                            CASE WHEN NULLIF(@atividade_encerrada_cliente, '') = '' THEN 'false' ELSE @atividade_encerrada_cliente END AS atividade_encerrada_cliente,
                                                            CASE WHEN NULLIF(@data_nasc_cliente, '') = '' THEN GETDATE() ELSE @data_nasc_cliente END AS data_nasc_cliente,
                                                            CASE WHEN NULLIF(@data_cad_cliente, '') = '' THEN GETDATE() ELSE @data_cad_cliente END AS data_cad_cliente,
                                                            CASE WHEN NULLIF(@data_mod_cliente, '') = '' THEN GETDATE() ELSE @data_mod_cliente END AS data_mod_cliente,
                                                            CASE WHEN NULLIF(@lixeira, '') = '' THEN 'Nao' ELSE @lixeira END AS lixeira                                    ) AS source
                                                    ON (target.id_cliente = source.id_cliente)
                                                    WHEN MATCHED THEN
                                                        UPDATE SET
                                                            target.id_registro = source.id_registro,
                                                            target.tipo_pessoa = source.tipo_pessoa,
                                                            target.tipo_cadastro = source.tipo_cadastro,
                                                            target.cnpj_cliente = source.cnpj_cliente,
                                                            target.rg_cliente = source.rg_cliente,
                                                            target.data_emissao_rg_cliente = source.data_emissao_rg_cliente,
                                                            target.orgao_expedidor_rg_cliente = source.orgao_expedidor_rg_cliente,
                                                            target.passaporte_cliente = source.passaporte_cliente,
                                                            target.estrangeiro_cliente = source.estrangeiro_cliente,
                                                            target.razao_cliente = source.razao_cliente,
                                                            target.fantasia_cliente = source.fantasia_cliente,
                                                            target.endereco_cliente = source.endereco_cliente,
                                                            target.numero_cliente = source.numero_cliente,
                                                            target.bairro_cliente = source.bairro_cliente,
                                                            target.complemento_cliente = source.complemento_cliente,
                                                            target.referencia_cliente = source.referencia_cliente,
                                                            target.cep_cliente = source.cep_cliente,
                                                            target.cidade_cliente = source.cidade_cliente,
                                                            target.cidade_cliente_cod = source.cidade_cliente_cod,
                                                            target.uf_cliente = source.uf_cliente,
                                                            target.tel_destinatario_cliente = source.tel_destinatario_cliente,
                                                            target.doc_destinatario_cliente = source.doc_destinatario_cliente,
                                                            target.nome_destinatario_cliente = source.nome_destinatario_cliente,
                                                            target.contato_cliente = source.contato_cliente,
                                                            target.fone_cliente = source.fone_cliente,
                                                            target.fone_contato_cliente = source.fone_contato_cliente,
                                                            target.fone_ramal_cliente = source.fone_ramal_cliente,
                                                            target.fax_cliente = source.fax_cliente,
                                                            target.celular_cliente = source.celular_cliente,
                                                            target.email_cliente = source.email_cliente,
                                                            target.email_contato_cliente = source.email_contato_cliente,
                                                            target.celular_contato_cliente = source.celular_contato_cliente,
                                                            target.estado_civil_cliente = source.estado_civil_cliente,
                                                            target.website_cliente = source.website_cliente,
                                                            target.aposentado_cliente = source.aposentado_cliente,
                                                            target.empregador_cliente = source.empregador_cliente,
                                                            target.profissao_cliente = source.profissao_cliente,
                                                            target.genero_cliente = source.genero_cliente,
                                                            target.insc_estadual_cliente = source.insc_estadual_cliente,
                                                            target.insc_municipal_cliente = source.insc_municipal_cliente,
                                                            target.insc_produtor_cliente = source.insc_produtor_cliente,
                                                            target.insc_suframa_cliente = source.insc_suframa_cliente,
                                                            target.nif = source.nif,
                                                            target.situacao_cliente = source.situacao_cliente,
                                                            target.vendedor_cliente = source.vendedor_cliente,
                                                            target.vendedor_cliente_id = source.vendedor_cliente_id,
                                                            target.modalidade_frete = source.modalidade_frete,
                                                            target.id_transportadora = source.id_transportadora,
                                                            target.desc_transportadora = source.desc_transportadora,
                                                            target.observacoes_cliente = source.observacoes_cliente,
                                                            target.listapreco_cliente = source.listapreco_cliente,
                                                            target.condicaopag_cliente = source.condicaopag_cliente,
                                                            target.limite_credito = source.limite_credito,
                                                            target.ultrapassar_limite_credito = source.ultrapassar_limite_credito,
                                                            target.consumidor_final = source.consumidor_final,
                                                            target.contribuinte_icms = source.contribuinte_icms,
                                                            target.atividade_encerrada_cliente = source.atividade_encerrada_cliente,
                                                            target.data_nasc_cliente = source.data_nasc_cliente,
                                                            target.data_mod_cliente = source.data_mod_cliente,
                                                            target.lixeira = source.lixeira
                                                    WHEN NOT MATCHED THEN
                                                        INSERT (
                                                            id_cliente, id_registro, tipo_pessoa, tipo_cadastro, cnpj_cliente, rg_cliente,
                                                            data_emissao_rg_cliente, orgao_expedidor_rg_cliente, passaporte_cliente, estrangeiro_cliente,
                                                            razao_cliente, fantasia_cliente, endereco_cliente, numero_cliente, bairro_cliente,
                                                            complemento_cliente, referencia_cliente, cep_cliente, cidade_cliente, cidade_cliente_cod,
                                                            uf_cliente, tel_destinatario_cliente, doc_destinatario_cliente, nome_destinatario_cliente,
                                                            contato_cliente, fone_cliente, fone_contato_cliente, fone_ramal_cliente, fax_cliente,
                                                            celular_cliente, email_cliente, email_contato_cliente, celular_contato_cliente,
                                                            estado_civil_cliente, website_cliente, aposentado_cliente, empregador_cliente,
                                                            profissao_cliente, genero_cliente, insc_estadual_cliente, insc_municipal_cliente,
                                                            insc_produtor_cliente, insc_suframa_cliente, nif, situacao_cliente, vendedor_cliente,
                                                            vendedor_cliente_id, modalidade_frete, id_transportadora, desc_transportadora, observacoes_cliente,
                                                            listapreco_cliente, condicaopag_cliente, limite_credito, ultrapassar_limite_credito,
                                                            consumidor_final, contribuinte_icms, atividade_encerrada_cliente, data_nasc_cliente, data_cad_cliente,
                                                            data_mod_cliente, lixeira
                                                        )
                                                        VALUES (
                                                            source.id_cliente, source.id_registro, source.tipo_pessoa, source.tipo_cadastro, source.cnpj_cliente, source.rg_cliente,
                                                            source.data_emissao_rg_cliente, source.orgao_expedidor_rg_cliente, source.passaporte_cliente, source.estrangeiro_cliente,
                                                            source.razao_cliente, source.fantasia_cliente, source.endereco_cliente, source.numero_cliente, source.bairro_cliente,
                                                            source.complemento_cliente, source.referencia_cliente, source.cep_cliente, source.cidade_cliente, source.cidade_cliente_cod,
                                                            source.uf_cliente, source.tel_destinatario_cliente, source.doc_destinatario_cliente, source.nome_destinatario_cliente,
                                                            source.contato_cliente, source.fone_cliente, source.fone_contato_cliente, source.fone_ramal_cliente, source.fax_cliente,
                                                            source.celular_cliente, source.email_cliente, source.email_contato_cliente, source.celular_contato_cliente,
                                                            source.estado_civil_cliente, source.website_cliente, source.aposentado_cliente, source.empregador_cliente,
                                                            source.profissao_cliente, source.genero_cliente, source.insc_estadual_cliente, source.insc_municipal_cliente,
                                                            source.insc_produtor_cliente, source.insc_suframa_cliente, source.nif, source.situacao_cliente, source.vendedor_cliente,
                                                            source.vendedor_cliente_id, source.modalidade_frete, source.id_transportadora, source.desc_transportadora, source.observacoes_cliente,
                                                            source.listapreco_cliente, source.condicaopag_cliente, source.limite_credito, source.ultrapassar_limite_credito,
                                                            source.consumidor_final, source.contribuinte_icms, source.atividade_encerrada_cliente, source.data_nasc_cliente, source.data_cad_cliente,
                                                            source.data_mod_cliente, source.lixeira
                                                        );
                                                ";

                                                // Executar a consulta MERGE na conexão do SQL Server
                                                using (SqlCommand mergeCmd = new SqlCommand(mergeC1Query, connection))
                                                {
                                                    // Defina o valor de @id_cliente com base na condição
                                                    string idClienteValue = cliente["id_cliente"]?.ToString();
                                                    if (string.IsNullOrWhiteSpace(idClienteValue) || !int.TryParse(idClienteValue, out int idCliente))
                                                    {
                                                        idCliente = 0; // Valor padrão para valores inválidos
                                                    }
                                                    mergeCmd.Parameters.AddWithValue("@id_cliente", cliente["id_cliente"].ToString());
                                                    

                                                    // Defina o valor de @id_registro com base na condição
                                                    string idRegistroValue = cliente["id_registro"]?.ToString();
                                                    if (string.IsNullOrWhiteSpace(idRegistroValue) || !int.TryParse(idRegistroValue, out int idRegistro))
                                                    {
                                                        idCliente = 0; // Valor padrão para valores inválidos
                                                    } 
                                                    mergeCmd.Parameters.AddWithValue("@id_registro", cliente["id_registro"].ToString());
                                                    

                                                    mergeCmd.Parameters.AddWithValue("@tipo_pessoa", cliente["tipo_pessoa"].ToString());
                                                    mergeCmd.Parameters.AddWithValue("@tipo_cadastro", cliente["tipo_cadastro"].ToString());
                                                    mergeCmd.Parameters.AddWithValue("@cnpj_cliente", cliente["cnpj_cliente"].ToString());
                                                    mergeCmd.Parameters.AddWithValue("@rg_cliente", cliente["rg_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@data_emissao_rg_cliente", cliente["data_emissao_rg_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@orgao_expedidor_rg_cliente", cliente["orgao_expedidor_rg_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@passaporte_cliente", cliente["passaporte_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@estrangeiro_cliente", cliente["estrangeiro_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@razao_cliente", cliente["razao_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fantasia_cliente", cliente["fantasia_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@endereco_cliente", cliente["endereco_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@numero_cliente", cliente["numero_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@bairro_cliente", cliente["bairro_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@complemento_cliente", cliente["complemento_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@referencia_cliente", cliente["referencia_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@cep_cliente", cliente["cep_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@cidade_cliente", cliente["cidade_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@cidade_cliente_cod", cliente["cidade_cliente_cod"]?.ToString() ?? "0");
                                                    mergeCmd.Parameters.AddWithValue("@uf_cliente", cliente["uf_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@tel_destinatario_cliente", cliente["tel_destinatario_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@doc_destinatario_cliente", cliente["doc_destinatario_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@nome_destinatario_cliente", cliente["nome_destinatario_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@contato_cliente", cliente["contato_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fone_cliente", cliente["fone_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fone_contato_cliente", cliente["fone_contato_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fone_ramal_cliente", cliente["fone_ramal_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@fax_cliente", cliente["fax_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@celular_cliente", cliente["celular_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@email_cliente", cliente["email_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@email_contato_cliente", cliente["email_contato_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@celular_contato_cliente", cliente["celular_contato_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@estado_civil_cliente", cliente["estado_civil_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@website_cliente", cliente["website_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@aposentado_cliente", cliente["aposentado_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@empregador_cliente", cliente["empregador_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@profissao_cliente", cliente["profissao_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@genero_cliente", cliente["genero_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@insc_estadual_cliente", cliente["insc_estadual_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@insc_municipal_cliente", cliente["insc_municipal_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@insc_produtor_cliente", cliente["insc_produtor_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@insc_suframa_cliente", cliente["insc_suframa_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@nif", cliente["nif"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@situacao_cliente", cliente["situacao_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@vendedor_cliente", cliente["vendedor_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@vendedor_cliente_id", cliente["vendedor_cliente_id"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@modalidade_frete", cliente["modalidade_frete"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@id_transportadora", cliente["id_transportadora"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@desc_transportadora", cliente["desc_transportadora"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@observacoes_cliente", cliente["observacoes_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@listapreco_cliente", cliente["listapreco_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@condicaopag_cliente", cliente["condicaopag_cliente"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@limite_credito", cliente["limite_credito"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@ultrapassar_limite_credito", cliente["ultrapassar_limite_credito"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@consumidor_final", cliente["consumidor_final"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@contribuinte_icms", cliente["contribuinte_icms"]?.ToString() ?? "null");
                                                    mergeCmd.Parameters.AddWithValue("@atividade_encerrada_cliente", cliente["atividade_encerrada_cliente"]?.ToString() ?? "null");

                                                    string dataNascClienteValue = cliente["data_nasc_cliente"]?.ToString();
                                                    if (string.IsNullOrWhiteSpace(dataNascClienteValue) || !DateTime.TryParse(dataNascClienteValue, out DateTime dataNascCliente) || dataNascCliente < SqlDateTime.MinValue.Value || dataNascCliente > SqlDateTime.MaxValue.Value)
                                                    {
                                                        dataNascCliente = SqlDateTime.MinValue.Value; // Valor padrão para datas inválidas
                                                    }
                                                    mergeCmd.Parameters.AddWithValue("@data_nasc_cliente", dataNascCliente);

                                                    mergeCmd.Parameters.AddWithValue("@data_cad_cliente", dataCadCliente);
                                                    mergeCmd.Parameters.AddWithValue("@data_mod_cliente", dataModCliente);
                                                    

                                                    // Defina o valor de @lixeira com base na condição
                                                    mergeCmd.Parameters.AddWithValue("@lixeira", (cliente["lixeira"].ToString().ToLower() == "Nao") ? false : true);
                                    
                                                    // linha utilizada para teste de resposta
                                                    foreach (SqlParameter parameter in mergeCmd.Parameters)
                                                    {
                                                        Console.WriteLine($"{parameter.ParameterName}: {parameter.Value}");
                                                    }

                                                    await mergeCmd.ExecuteNonQueryAsync();
                                                    Console.WriteLine("Cliente inserido/atualizado modificação com sucesso na tabela C1!");
                                                }
                                            }
                                        }
                                        else
                                        {
                                            // Mudar o tipo de busca quando necessário
                                            if (buscarPorDataCadastro)
                                            {
                                                buscarPorDataCadastro = false; // Trocar para buscar por data de modificação
                                                // Reiniciar o offset para a próxima busca
                                                offset = 0;
                                            }
                                            else
                                            {
                                                Console.WriteLine("Clientes de hoje inseridos. Parando a execução do código.");
                                                break; // Finaliza o loop quando ambos os critérios são atendidos
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Erro ao processar cliente: {ex.Message}");
                                        Console.WriteLine($"Campo problemático: {IdentificarCampoProblematico(cliente)}");
                                    }
                                }
                            }
                            else
                            {
                                Console.WriteLine("Dados da API não encontrados ou em formato incorreto.");
                                break;
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Erro na requisição HTTP: {response.StatusCode}");
                            break;
                        }
                    }

                    offset += pageSize;
                }

                AtualizarDataUltimaAtualizacaoC1(connection, "ultima_atualizacao_C1");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro: {ex.Message}");
        }
    }

    static DateTime? GetDateTimeOrNull(JToken token, string propertyName)
    {
        if (token[propertyName] != null)
        {
            if (DateTime.TryParse(token[propertyName].ToString(), out DateTime result))
            {
                return result;
            }
        }
        return null;
    }

    // Função para obter um valor decimal ou null
    static decimal? GetDecimalOrNull(JToken token, string propertyName)
    {
        if (token[propertyName] != null)
        {
            if (decimal.TryParse(token[propertyName].ToString(), out decimal result))
            {
                return result;
            }
        }
        return null; // Retorna null se o campo estiver ausente ou nulo
    }

    // Função para obter um valor inteiro ou null
    static int? GetIntOrNull(JToken token, string propertyName)
    {
        if (token[propertyName] != null)
        {
            if (int.TryParse(token[propertyName].ToString(), out int result))
            {
                return result;
            }
        }
        return null;
    }

    static string IdentificarCampoProblematico(JToken pedido)
    {
        // Verifique cada campo no pedido e retorne o nome do campo que pode estar causando o erro
        foreach (var property in pedido.Children())
        {
            try
            {
                // Tente converter o valor do campo para DateTime
                if (property != null && property.HasValues && DateTime.TryParse(property.First.ToString(), out DateTime dateValue))
                {
                    continue; // Este campo não está causando o erro
                }
            }
            catch (Exception)
            {
                // Ignorar exceções de conversão aqui
            }
        }

        // Se nenhum campo causar o erro, retorne uma mensagem genérica
        return "Campo não identificado";
    }

    // Função para obter um valor booleano
    static bool GetBoolean(JToken token, string propertyName)
    {
        if (token[propertyName] != null)
        {
            return token[propertyName].ToString().Equals("Sim", StringComparison.OrdinalIgnoreCase);
        }
        return false;
    }

    // Função para obter um valor string ou null
    static string GetStringOrNull(JToken token, string propertyName)
    {
        if (token[propertyName] != null)
        {
            return token[propertyName].ToString();
        }
        return null;
    }

    // Função para obter a data da última execução da tabela de controle (CTRJOBS)
    static DateTime ObterDataUltimaAtualizacaoC1(SqlConnection connection, string campo)
    {
        using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = $"SELECT {campo} FROM CTRJOBS";
            object result = command.ExecuteScalar();
            if (result != DBNull.Value && result != null)
            {
                return (DateTime)result;
            }
            // Retorna uma data mínima se não houver valor na tabela de controle
            return DateTime.MinValue;
        }
    }

    // Função para atualizar a data da última execução na tabela de controle (CTRJOBS)
    static void AtualizarDataUltimaAtualizacaoC1(SqlConnection connection, string campo)
    {
        DateTime dataUltimaAtualizacaoC1 = DateTime.Now; // Defina a data e hora da atualização

        using (SqlCommand command = connection.CreateCommand())
        {
            // Atualize a coluna apropriada com a data e hora da atualização
            command.CommandText = $"UPDATE CTRJOBS SET {campo} = @dataUltimaAtualizacaoC1";
            command.Parameters.AddWithValue("@dataUltimaAtualizacaoC1", dataUltimaAtualizacaoC1);
            command.ExecuteNonQuery();
        }
    }
}

