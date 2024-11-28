using System;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

public class NC1
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

                string apiAddress = "https://api.vhsys.com/v2/notas-consumidor";
                string accessToken = "token";
                string secretAccessToken = "secret";
                DateTime dataAtual = DateTime.Now;

                int pageSize = 250; // Tamanho da página
                int offset = 0;
                DateTime dataLimite = DateTime.Today;

                // Obter a data da última atualização para o campo "ultima_atualizacao_NC1"
                DateTime dataUltimaAtualizacaoNC1 = ObterDataUltimaAtualizacaoNC1(connection, "ultima_atualizacao_NC1");

                while (true)
                {
                    using (HttpClient httpClient = new HttpClient())
                    {
                        // Adicione os cabeçalhos à solicitação HTTP
                        httpClient.DefaultRequestHeaders.Add("Access-Token", accessToken);
                        httpClient.DefaultRequestHeaders.Add("Secret-Access-Token", secretAccessToken);

                        string url = $"https://api.vhsys.com/v2/notas-consumidor?offset={offset}&limit={pageSize}&order=data_mod_pedido&sort=Desc";
                        //HttpResponseMessage pedidosResponse = await client.GetAsync($"https://api.vhsys.com/v2/pedidos?offset={offset}&limit={pageSize}&order=data_mod_pedido&sort=Desc"); // subir tudo
                        HttpResponseMessage response = await httpClient.GetAsync(url);

                        if (response.IsSuccessStatusCode)
                        {
                            string jsonResponse = await response.Content.ReadAsStringAsync();
                            JObject data = JObject.Parse(jsonResponse);

                            if (data["data"] is JArray dataArray)
                            {
                                int numberOfNotas = dataArray.Count;
                                Console.WriteLine($"Número de notas consumidos na página offset {offset}: {numberOfNotas}");

                                foreach (var notas in dataArray)
                                {
                                    try
                                    {
                                        // Extrair os dados de entrada da API
                                        int id_nfc = GetIntOrNull(notas, "id_nfc") ?? 0;
                                        int serie_nota = GetIntOrNull(notas, "serie_nota") ?? 0;
                                        int id_pedido = GetIntOrNull(notas, "id_pedido") ?? 0;
                                        int id_cliente = GetIntOrNull(notas, "id_cliente") ?? 0;
                                        string nome_cliente = GetStringOrNull(notas, "nome_cliente");
                                        string cliente_inexistente = GetStringOrNull(notas, "cliente_inexistente");
                                        int id_local_cobranca = GetIntOrNull(notas, "id_local_cobranca") ?? 0;
                                        string vendedor_pedido = GetStringOrNull(notas, "vendedor_pedido");
                                        int vendedor_pedido_id = GetIntOrNull(notas, "vendedor_pedido_id") ?? 0;
                                        decimal valor_total_produtos = GetDecimalOrNull(notas, "valor_total_produtos") ?? 0.00M;
                                        decimal desconto_pedido = GetDecimalOrNull(notas, "desconto_pedido") ?? 0.00M;
                                        decimal peso_total_nota = GetDecimalOrNull(notas, "peso_total_nota") ?? 0.00M;
                                        decimal peso_total_nota_liq = GetDecimalOrNull(notas, "peso_total_nota_liq") ?? 0.00M;
                                        decimal frete_pedido = GetDecimalOrNull(notas, "frete_pedido") ?? 0.00M;
                                        decimal valor_total_nota = GetDecimalOrNull(notas, "valor_total_nota") ?? 0.00M;
                                        decimal valor_baseICMS = GetDecimalOrNull(notas, "valor_baseICMS") ?? 0.00M;
                                        decimal valor_ICMS = GetDecimalOrNull(notas, "valor_ICMS") ?? 0.00M;
                                        decimal valor_despesas = GetDecimalOrNull(notas, "valor_despesas") ?? 0.00M;
                                        int condicao_pagamento = GetIntOrNull(notas, "condicao_pagamento") ?? 0;
                                        int frete_por_pedido = GetIntOrNull(notas, "frete_por_pedido") ?? 0;
                                        string transportadora_pedido = GetStringOrNull(notas, "transportadora_pedido");
                                        int id_transportadora = GetIntOrNull(notas, "id_transportadora") ?? 0;
                                        int volumes_transporta = GetIntOrNull(notas, "volumes_transporta") ?? 0;
                                        string especie_transporta = GetStringOrNull(notas, "especie_transporta");
                                        string marca_transporta = GetStringOrNull(notas, "marca_transporta");
                                        string numeracao_transporta = GetStringOrNull(notas, "numeracao_transporta");
                                        string placa_transporta = GetStringOrNull(notas, "placa_transporta");
                                        int indPres_pedido = GetIntOrNull(notas, "indPres_pedido") ?? 0;
                                        string natureza_pedido = GetStringOrNull(notas, "natureza_pedido");
                                        int cfop_pedido = GetIntOrNull(notas, "cfop_pedido") ?? 0;
                                        string obs_pedido = GetStringOrNull(notas, "obs_pedido");
                                        string obs_interno_pedido = GetStringOrNull(notas, "obs_interno_pedido");
                                        string status_pedido = GetStringOrNull(notas, "status_pedido");
                                        int contas_pedido = GetIntOrNull(notas, "contas_pedido") ?? 0;
                                        int comissao_pedido = GetIntOrNull(notas, "comissao_pedido") ?? 0;
                                        int estoque_pedido = GetIntOrNull(notas, "estoque_pedido") ?? 0;
                                        int nota_emitida = GetIntOrNull(notas, "nota_emitida") ?? 0;
                                        string nota_chave = GetStringOrNull(notas, "nota_chave");
                                        string nota_protocolo = GetStringOrNull(notas, "nota_protocolo");
                                        string nota_recibo = GetStringOrNull(notas, "nota_recibo");
                                        string nota_data_autorizacao = GetStringOrNull(notas, "nota_data_autorizacao");
                                        string nota_usuario_autorizacao = GetStringOrNull(notas, "nota_usuario_autorizacao");
                                        string nota_data_cancelamento = GetStringOrNull(notas, "nota_data_cancelamento");
                                        string nota_usuario_cancelamento = GetStringOrNull(notas, "nota_usuario_cancelamento");
                                        string nota_motivo_cancelamento = GetStringOrNull(notas, "nota_motivo_cancelamento");
                                        string nota_denegada = GetStringOrNull(notas, "nota_denegada");
                                        int id_almoxarifado = GetIntOrNull(notas, "id_almoxarifado") ?? 0;
                                        DateTime? data_cad_pedido = GetDateTimeOrNull(notas, "data_cad_pedido");
                                        DateTime? data_mod_pedido = GetDateTimeOrNull(notas, "data_mod_pedido");
                                        int ambiente = GetIntOrNull(notas, "ambiente") ?? 0;
                                        bool lixeira = GetBoolean(notas, "lixeira");
                                        string url_qr_code = GetStringOrNull(notas, "url_qr_code");
                                        string tipo_intermediador = GetStringOrNull(notas, "tipo_intermediador");
                                        string cnpj_intermediador = GetStringOrNull(notas, "cnpj_intermediador");
                                        string ident_no_intermediador = GetStringOrNull(notas, "ident_no_intermediador");
                                        int id_pedido_ref = GetIntOrNull(notas, "id_pedido_ref") ?? 0;
                                        string qBCMono = GetStringOrNull(notas, "qBCMono");
                                        string vICMSMono = GetStringOrNull(notas, "vICMSMono");
                                        string qBCMonoReten = GetStringOrNull(notas, "qBCMonoReten");
                                        string vICMSMonoReten = GetStringOrNull(notas, "vICMSMonoReten");
                                        string qBCMonoRet = GetStringOrNull(notas, "qBCMonoRet");
                                        string vICMSMonoRet = GetStringOrNull(notas, "vICMSMonoRet");

                                        if (data_mod_pedido >= dataLimite)
                                        {
                                        using (SqlCommand command = connection.CreateCommand())
                                        {
                                            // Construa a consulta MERGE para a tabela NC1
                                            string mergeNC1Query = @"
                                            MERGE INTO NC1 AS target
                                            USING (SELECT @id_nfc AS id_nfc) AS source
                                            ON (target.id_nfc = source.id_nfc)
                                            WHEN MATCHED THEN
                                                UPDATE SET
                                                    target.serie_nota = @serie_nota,
                                                    target.id_pedido = @id_pedido,
                                                    target.id_cliente = @id_cliente,
                                                    target.nome_cliente = @nome_cliente,
                                                    target.cliente_inexistente = @cliente_inexistente,
                                                    target.id_local_cobranca = @id_local_cobranca,
                                                    target.vendedor_pedido = @vendedor_pedido,
                                                    target.vendedor_pedido_id = @vendedor_pedido_id,
                                                    target.valor_total_produtos = @valor_total_produtos,
                                                    target.desconto_pedido = @desconto_pedido,
                                                    target.peso_total_nota = @peso_total_nota,
                                                    target.peso_total_nota_liq = @peso_total_nota_liq,
                                                    target.frete_pedido = @frete_pedido,
                                                    target.valor_total_nota = @valor_total_nota,
                                                    target.valor_baseICMS = @valor_baseICMS,
                                                    target.valor_ICMS = @valor_ICMS,
                                                    target.valor_despesas = @valor_despesas,
                                                    target.condicao_pagamento = @condicao_pagamento,
                                                    target.frete_por_pedido = @frete_por_pedido,
                                                    target.transportadora_pedido = @transportadora_pedido,
                                                    target.id_transportadora = @id_transportadora,
                                                    target.volumes_transporta = @volumes_transporta,
                                                    target.especie_transporta = @especie_transporta,
                                                    target.marca_transporta = @marca_transporta,
                                                    target.numeracao_transporta = @numeracao_transporta,
                                                    target.placa_transporta = @placa_transporta,
                                                    target.indPres_pedido = @indPres_pedido,
                                                    target.natureza_pedido = @natureza_pedido,
                                                    target.cfop_pedido = @cfop_pedido,
                                                    target.obs_pedido = @obs_pedido,
                                                    target.obs_interno_pedido = @obs_interno_pedido,
                                                    target.status_pedido = @status_pedido,
                                                    target.contas_pedido = @contas_pedido,
                                                    target.comissao_pedido = @comissao_pedido,
                                                    target.estoque_pedido = @estoque_pedido,
                                                    target.nota_emitida = @nota_emitida,
                                                    target.nota_chave = @nota_chave,
                                                    target.nota_protocolo = @nota_protocolo,
                                                    target.nota_recibo = @nota_recibo,
                                                    target.nota_data_autorizacao = @nota_data_autorizacao,
                                                    target.nota_usuario_autorizacao = @nota_usuario_autorizacao,
                                                    target.nota_data_cancelamento = @nota_data_cancelamento,
                                                    target.nota_usuario_cancelamento = @nota_usuario_cancelamento,
                                                    target.nota_motivo_cancelamento = @nota_motivo_cancelamento,
                                                    target.nota_denegada = @nota_denegada,
                                                    target.id_almoxarifado = @id_almoxarifado,
                                                    target.data_cad_pedido = COALESCE(NULLIF(@data_cad_pedido, ''), GETDATE()),
                                                    target.data_mod_pedido = COALESCE(NULLIF(@data_mod_pedido, ''), GETDATE()),
                                                    target.ambiente = @ambiente,
                                                    target.lixeira = @lixeira,
                                                    target.url_qr_code = @url_qr_code,
                                                    target.tipo_intermediador = @tipo_intermediador,
                                                    target.cnpj_intermediador = @cnpj_intermediador,
                                                    target.ident_no_intermediador = @ident_no_intermediador,
                                                    target.id_pedido_ref = @id_pedido_ref,
                                                    target.qBCMono = @qBCMono,
                                                    target.vICMSMono = @vICMSMono,
                                                    target.qBCMonoReten = @qBCMonoReten,
                                                    target.vICMSMonoReten = @vICMSMonoReten,
                                                    target.qBCMonoRet = @qBCMonoRet,
                                                    target.vICMSMonoRet = @vICMSMonoRet
                                                    WHEN NOT MATCHED THEN
                                                    INSERT (id_nfc, serie_nota, id_pedido, id_cliente, nome_cliente, cliente_inexistente, id_local_cobranca, vendedor_pedido, vendedor_pedido_id, valor_total_produtos, desconto_pedido, peso_total_nota, peso_total_nota_liq, frete_pedido, valor_total_nota, valor_baseICMS, valor_ICMS, valor_despesas, condicao_pagamento, frete_por_pedido, transportadora_pedido, id_transportadora, volumes_transporta, especie_transporta, marca_transporta, numeracao_transporta, placa_transporta, indPres_pedido, natureza_pedido, cfop_pedido, obs_pedido, obs_interno_pedido, status_pedido, contas_pedido, comissao_pedido, estoque_pedido, nota_emitida, nota_chave, nota_protocolo, nota_recibo, nota_data_autorizacao, nota_usuario_autorizacao, nota_data_cancelamento, nota_usuario_cancelamento, nota_motivo_cancelamento, nota_denegada, id_almoxarifado, data_cad_pedido, data_mod_pedido, ambiente, lixeira, url_qr_code, tipo_intermediador, cnpj_intermediador, ident_no_intermediador, id_pedido_ref, qBCMono, vICMSMono, qBCMonoReten, vICMSMonoReten, qBCMonoRet, vICMSMonoRet)
                                                    VALUES (@id_nfc, @serie_nota, @id_pedido, @id_cliente, @nome_cliente, @cliente_inexistente, @id_local_cobranca, @vendedor_pedido, @vendedor_pedido_id, @valor_total_produtos, @desconto_pedido, @peso_total_nota, @peso_total_nota_liq, @frete_pedido, @valor_total_nota, @valor_baseICMS, @valor_ICMS, @valor_despesas, @condicao_pagamento, @frete_por_pedido, @transportadora_pedido, @id_transportadora, @volumes_transporta, @especie_transporta, @marca_transporta, @numeracao_transporta, @placa_transporta, @indPres_pedido, @natureza_pedido, @cfop_pedido, @obs_pedido, @obs_interno_pedido, @status_pedido, @contas_pedido, @comissao_pedido, @estoque_pedido, @nota_emitida, @nota_chave, @nota_protocolo, @nota_recibo, @nota_data_autorizacao, @nota_usuario_autorizacao, @nota_data_cancelamento, @nota_usuario_cancelamento, @nota_motivo_cancelamento, @nota_denegada, @id_almoxarifado, COALESCE(NULLIF(@data_cad_pedido, ''), GETDATE()), COALESCE(NULLIF(@data_mod_pedido, ''), GETDATE()), @ambiente, @lixeira, @url_qr_code, @tipo_intermediador, @cnpj_intermediador, @ident_no_intermediador, @id_pedido_ref, @qBCMono, @vICMSMono, @qBCMonoReten, @vICMSMonoReten, @qBCMonoRet, @vICMSMonoRet);
                                            ";
                                                

                                            // Executar a consulta MERGE na conexão do SQL Server
                                            using (SqlCommand mergeCmd = new SqlCommand(mergeNC1Query, connection))
                                            {
                                                mergeCmd.Parameters.AddWithValue("@id_nfc", id_nfc);
                                                mergeCmd.Parameters.AddWithValue("@serie_nota", serie_nota);
                                                mergeCmd.Parameters.AddWithValue("@id_pedido", id_pedido);
                                                mergeCmd.Parameters.AddWithValue("@id_cliente", id_cliente);
                                                mergeCmd.Parameters.AddWithValue("@nome_cliente", nome_cliente);
                                                mergeCmd.Parameters.AddWithValue("@cliente_inexistente", cliente_inexistente);
                                                mergeCmd.Parameters.AddWithValue("@id_local_cobranca", id_local_cobranca);
                                                mergeCmd.Parameters.AddWithValue("@vendedor_pedido", vendedor_pedido);
                                                mergeCmd.Parameters.AddWithValue("@vendedor_pedido_id", vendedor_pedido_id);
                                                mergeCmd.Parameters.AddWithValue("@valor_total_produtos", valor_total_produtos);
                                                mergeCmd.Parameters.AddWithValue("@desconto_pedido", desconto_pedido);
                                                mergeCmd.Parameters.AddWithValue("@peso_total_nota", peso_total_nota);
                                                mergeCmd.Parameters.AddWithValue("@peso_total_nota_liq", peso_total_nota_liq);
                                                mergeCmd.Parameters.AddWithValue("@frete_pedido", frete_pedido);
                                                mergeCmd.Parameters.AddWithValue("@valor_total_nota", valor_total_nota);
                                                mergeCmd.Parameters.AddWithValue("@valor_baseICMS", valor_baseICMS);
                                                mergeCmd.Parameters.AddWithValue("@valor_ICMS", valor_ICMS);
                                                mergeCmd.Parameters.AddWithValue("@valor_despesas", valor_despesas);
                                                mergeCmd.Parameters.AddWithValue("@condicao_pagamento", condicao_pagamento);
                                                mergeCmd.Parameters.AddWithValue("@frete_por_pedido", frete_por_pedido);
                                                mergeCmd.Parameters.AddWithValue("@transportadora_pedido", transportadora_pedido);
                                                mergeCmd.Parameters.AddWithValue("@id_transportadora", id_transportadora);
                                                mergeCmd.Parameters.AddWithValue("@volumes_transporta", volumes_transporta);
                                                mergeCmd.Parameters.AddWithValue("@especie_transporta", especie_transporta);
                                                mergeCmd.Parameters.AddWithValue("@marca_transporta", marca_transporta);
                                                mergeCmd.Parameters.AddWithValue("@numeracao_transporta", numeracao_transporta);
                                                mergeCmd.Parameters.AddWithValue("@placa_transporta", placa_transporta);
                                                mergeCmd.Parameters.AddWithValue("@indPres_pedido", indPres_pedido);
                                                mergeCmd.Parameters.AddWithValue("@natureza_pedido", natureza_pedido);
                                                mergeCmd.Parameters.AddWithValue("@cfop_pedido", cfop_pedido);
                                                mergeCmd.Parameters.AddWithValue("@obs_pedido", obs_pedido);
                                                mergeCmd.Parameters.AddWithValue("@obs_interno_pedido", obs_interno_pedido);
                                                mergeCmd.Parameters.AddWithValue("@status_pedido", status_pedido);
                                                mergeCmd.Parameters.AddWithValue("@contas_pedido", contas_pedido);
                                                mergeCmd.Parameters.AddWithValue("@comissao_pedido", comissao_pedido);
                                                mergeCmd.Parameters.AddWithValue("@estoque_pedido", estoque_pedido);
                                                mergeCmd.Parameters.AddWithValue("@nota_emitida", nota_emitida);
                                                mergeCmd.Parameters.AddWithValue("@nota_chave", nota_chave);
                                                mergeCmd.Parameters.AddWithValue("@nota_protocolo", nota_protocolo);
                                                mergeCmd.Parameters.AddWithValue("@nota_recibo", nota_recibo);
                                                mergeCmd.Parameters.AddWithValue("@nota_data_autorizacao", nota_data_autorizacao);
                                                mergeCmd.Parameters.AddWithValue("@nota_usuario_autorizacao", nota_usuario_autorizacao);
                                                mergeCmd.Parameters.AddWithValue("@nota_data_cancelamento", nota_data_cancelamento);
                                                mergeCmd.Parameters.AddWithValue("@nota_usuario_cancelamento", nota_usuario_cancelamento);
                                                mergeCmd.Parameters.AddWithValue("@nota_motivo_cancelamento", nota_motivo_cancelamento);
                                                mergeCmd.Parameters.AddWithValue("@nota_denegada", nota_denegada);
                                                mergeCmd.Parameters.AddWithValue("@id_almoxarifado", id_almoxarifado);
                                                mergeCmd.Parameters.AddWithValue("@data_cad_pedido", data_cad_pedido);
                                                mergeCmd.Parameters.AddWithValue("@data_mod_pedido", data_mod_pedido);
                                                mergeCmd.Parameters.AddWithValue("@ambiente", ambiente);
                                                mergeCmd.Parameters.AddWithValue("@lixeira", lixeira);
                                                mergeCmd.Parameters.AddWithValue("@url_qr_code", url_qr_code);
                                                mergeCmd.Parameters.AddWithValue("@tipo_intermediador", tipo_intermediador);
                                                mergeCmd.Parameters.AddWithValue("@cnpj_intermediador", cnpj_intermediador);
                                                mergeCmd.Parameters.AddWithValue("@ident_no_intermediador", ident_no_intermediador);
                                                mergeCmd.Parameters.AddWithValue("@id_pedido_ref", id_pedido_ref);
                                                mergeCmd.Parameters.AddWithValue("@qBCMono", qBCMono);
                                                mergeCmd.Parameters.AddWithValue("@vICMSMono", vICMSMono);
                                                mergeCmd.Parameters.AddWithValue("@qBCMonoReten", qBCMonoReten);
                                                mergeCmd.Parameters.AddWithValue("@vICMSMonoReten", vICMSMonoReten);
                                                mergeCmd.Parameters.AddWithValue("@qBCMonoRet", qBCMonoRet);
                                                mergeCmd.Parameters.AddWithValue("@vICMSMonoRet", vICMSMonoRet);


                                                await mergeCmd.ExecuteNonQueryAsync();
                                                Console.WriteLine("Nota do consumidor inserida/atualizada com sucesso na tabela NC1!");
                                                }
                                            }
                                        }

                                        else
                                        {
                                            Console.WriteLine("Pedidos de hoje inseridos. Parando a execução do código.");
                                            Environment.Exit(0); // Isso vai encerrar o programa
                                            break;
                                        }

                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Erro ao processar pedido: {ex.Message}");
                                        Console.WriteLine($"Campo problemático: {IdentificarCampoProblematico(notas)}");
                                    }
                                }
                            }
                            else
                            {
                                Console.WriteLine("Dados da API não encontrados ou em formato incorreto.");
                                break; // Sai do loop em caso de erro
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Erro na requisição HTTP: {response.StatusCode}");
                            break; // Sai do loop em caso de erro
                        }
                    }

                    // Avança para a próxima página
                    offset += pageSize;
                }

                // Atualize a data da última execução após o término da sincronização
                AtualizarDataUltimaAtualizacaoNC1(connection, "ultima_atualizacao_NC1");
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
    static DateTime ObterDataUltimaAtualizacaoNC1(SqlConnection connection, string campo)
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
    static void AtualizarDataUltimaAtualizacaoNC1(SqlConnection connection, string campo)
    {
        DateTime dataUltimaAtualizacaoNC1 = DateTime.Now; // Defina a data e hora da atualização

        using (SqlCommand command = connection.CreateCommand())
        {
            // Atualize a coluna apropriada com a data e hora da atualização
            command.CommandText = $"UPDATE CTRJOBS SET {campo} = @dataUltimaAtualizacaoNC1";
            command.Parameters.AddWithValue("@dataUltimaAtualizacaoNC1", dataUltimaAtualizacaoNC1);
            command.ExecuteNonQuery();
        }
    }
}