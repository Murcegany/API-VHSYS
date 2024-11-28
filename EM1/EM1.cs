using System;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

public class EM1
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

                string apiAddress = "https://api.vhsys.com/v2/entradas-mercadoria";
                string accessToken = "token";
                string secretAccessToken = "secret";
                DateTime dataLimite = DateTime.Today;

                int pageSize = 250; // Tamanho da página
                int offset = 0;

                // Obter a data da última atualização para o campo "ultima_atualizacao_EM1"
                DateTime dataUltimaAtualizacaoEM1 = ObterDataUltimaAtualizacaoEM1(connection, "ultima_atualizacao_EM1");

                while (true)
                {
                    using (HttpClient httpClient = new HttpClient())
                    {
                        // Adicione os cabeçalhos à solicitação HTTP
                        httpClient.DefaultRequestHeaders.Add("Access-Token", accessToken);
                        httpClient.DefaultRequestHeaders.Add("Secret-Access-Token", secretAccessToken);

                        string url = $"https://api.vhsys.com/v2/entradas-mercadoria?offset={offset}&limit={pageSize}&order=data_cad_pedido&sort=Desc";
                        //HttpResponseMessage pedidosResponse = await client.GetAsync($"https://api.vhsys.com/v2/pedidos?offset={offset}&limit={pageSize}&order=data_mod_pedido&sort=Desc"); // subir tudo
                        HttpResponseMessage response = await httpClient.GetAsync(url);

                        if (response.IsSuccessStatusCode)
                        {
                            string jsonResponse = await response.Content.ReadAsStringAsync();
                            JObject data = JObject.Parse(jsonResponse);

                            if (data["data"] is JArray dataArray)
                            {
                                int numberOfEntradas = dataArray.Count;
                                Console.WriteLine($"Número de entradas na página offset {offset}: {numberOfEntradas}");

                                foreach (var entrada in dataArray)
                                {
                                    try
                                    {
                                        // Extrair os dados de entrada da API
                                        int id_entrada = GetIntOrNull(entrada, "id_entrada") ?? 0;
                                        int id_pedido = GetIntOrNull(entrada, "id_pedido") ?? 0;
                                        int id_cliente = GetIntOrNull(entrada, "id_cliente") ?? 0;
                                        string nome_cliente = GetStringOrNull(entrada, "nome_cliente");
                                        string vendedor_pedido = GetStringOrNull(entrada, "vendedor_pedido");
                                        decimal valor_total_produtos = GetDecimalOrNull(entrada, "valor_total_produtos") ?? 0.00M;
                                        decimal desconto_pedido = GetDecimalOrNull(entrada, "desconto_pedido") ?? 0.00M;
                                        decimal peso_total_nota = GetDecimalOrNull(entrada, "peso_total_nota") ?? 0.00M;
                                        decimal frete_pedido = GetDecimalOrNull(entrada, "frete_pedido") ?? 0.00M;
                                        decimal valor_total_nota = GetDecimalOrNull(entrada, "valor_total_nota") ?? 0.00M;
                                        decimal valor_baseICMS = GetDecimalOrNull(entrada, "valor_baseICMS") ?? 0.00M;
                                        decimal valor_ICMS = GetDecimalOrNull(entrada, "valor_ICMS") ?? 0.00M;
                                        decimal valor_baseST = GetDecimalOrNull(entrada, "valor_baseST") ?? 0.00M;
                                        decimal valor_ST = GetDecimalOrNull(entrada, "valor_ST") ?? 0.00M;
                                        decimal valor_IPI = GetDecimalOrNull(entrada, "valor_IPI") ?? 0.00M;
                                        decimal valor_PIS = GetDecimalOrNull(entrada, "valor_PIS") ?? 0.00M;
                                        decimal valor_COFINS = GetDecimalOrNull(entrada, "valor_COFINS") ?? 0.00M;
                                        int condicao_pagamento = GetIntOrNull(entrada, "condicao_pagamento") ?? 0;
                                        string transportadora_pedido = GetStringOrNull(entrada, "transportadora_pedido");
                                        int id_transportadora = GetIntOrNull(entrada, "id_transportadora") ?? 0;
                                        DateTime? data_pedido = GetDateTimeOrNull(entrada, "data_pedido");
                                        int id_centro_custos = GetIntOrNull(entrada, "id_centro_custos") ?? 0;
                                        string centro_custos_pedido = GetStringOrNull(entrada, "centro_custos_pedido");
                                        string obs_pedido = GetStringOrNull(entrada, "obs_pedido");
                                        string obs_interno_pedido = GetStringOrNull(entrada, "obs_interno_pedido");
                                        string status_pedido = GetStringOrNull(entrada, "status_pedido");
                                        int contas_pedido = GetIntOrNull(entrada, "contas_pedido") ?? 0;
                                        int estoque_pedido = GetIntOrNull(entrada, "estoque_pedido") ?? 0;
                                        int nota_numero = GetIntOrNull(entrada, "nota_numero") ?? 0;
                                        string nota_chave = GetStringOrNull(entrada, "nota_chave");
                                        string nota_protocolo = GetStringOrNull(entrada, "nota_protocolo");
                                        DateTime? nota_data_autorizacao = GetDateTimeOrNull(entrada, "nota_data_autorizacao");
                                        DateTime? data_cad_pedido = GetDateTimeOrNull(entrada, "data_cad_pedido");
                                        DateTime? data_mod_pedido = GetDateTimeOrNull(entrada, "data_mod_pedido");
                                        string modelo_nota = GetStringOrNull(entrada, "modelo_nota");
                                        string serie_nota = GetStringOrNull(entrada, "serie_nota");
                                        int importacao = GetIntOrNull(entrada, "importacao") ?? 0;
                                        string almoxarifado = GetStringOrNull(entrada, "almoxarifado");
                                        bool lixeira = GetBoolean(entrada, "lixeira");
                                        string usuario_cad_pedido = GetStringOrNull(entrada, "usuario_cad_pedido");
                                        string usuario_mod_pedido = GetStringOrNull(entrada, "usuario_mod_pedido");

                                        if (data_cad_pedido >= dataLimite)
                                        {
                                        using (SqlCommand command = connection.CreateCommand())
                                        {
                                            // Construa a consulta MERGE para a tabela EM1
                                            string mergeEM1Query = @"
                                                MERGE INTO EM1 AS target
                                                USING (SELECT @id_entrada AS id_entrada) AS source
                                                ON (target.id_entrada = source.id_entrada)
                                                WHEN MATCHED THEN
                                                UPDATE SET
                                                    target.id_pedido = @id_pedido,
                                                    target.id_cliente = @id_cliente,
                                                    target.nome_cliente = @nome_cliente,
                                                    target.vendedor_pedido = @vendedor_pedido,
                                                    target.valor_total_produtos = @valor_total_produtos,
                                                    target.desconto_pedido = @desconto_pedido,
                                                    target.peso_total_nota = @peso_total_nota,
                                                    target.frete_pedido = @frete_pedido,
                                                    target.valor_total_nota = @valor_total_nota,
                                                    target.valor_baseICMS = @valor_baseICMS,
                                                    target.valor_ICMS = @valor_ICMS,
                                                    target.valor_baseST = @valor_baseST,
                                                    target.valor_ST = @valor_ST,
                                                    target.valor_IPI = @valor_IPI,
                                                    target.valor_PIS = @valor_PIS,
                                                    target.valor_COFINS = @valor_COFINS,
                                                    target.condicao_pagamento = @condicao_pagamento,
                                                    target.transportadora_pedido = @transportadora_pedido,
                                                    target.id_transportadora = @id_transportadora,
                                                    target.data_pedido = @data_pedido,
                                                    target.id_centro_custos = @id_centro_custos,
                                                    target.centro_custos_pedido = @centro_custos_pedido,
                                                    target.obs_pedido = @obs_pedido,
                                                    target.obs_interno_pedido = @obs_interno_pedido,
                                                    target.status_pedido = @status_pedido,
                                                    target.contas_pedido = @contas_pedido,
                                                    target.estoque_pedido = @estoque_pedido,
                                                    target.nota_numero = @nota_numero,
                                                    target.nota_chave = @nota_chave,
                                                    target.nota_protocolo = @nota_protocolo,
                                                    target.nota_data_autorizacao = @nota_data_autorizacao,
                                                    target.data_cad_pedido = COALESCE(NULLIF(@data_cad_pedido, ''), GETDATE()),
                                                    target.data_mod_pedido = COALESCE(NULLIF(@data_mod_pedido, ''), GETDATE()),
                                                    target.modelo_nota = @modelo_nota,
                                                    target.serie_nota = @serie_nota,
                                                    target.importacao = @importacao,
                                                    target.almoxarifado = @almoxarifado,
                                                    target.lixeira = @lixeira,
                                                    target.usuario_cad_pedido = @usuario_cad_pedido,
                                                    target.usuario_mod_pedido = @usuario_mod_pedido
                                                    WHEN NOT MATCHED THEN
                                                        INSERT (id_entrada, id_pedido, id_cliente, nome_cliente, vendedor_pedido, valor_total_produtos, desconto_pedido, peso_total_nota, frete_pedido, valor_total_nota, valor_baseICMS, valor_ICMS, valor_baseST, valor_ST, valor_IPI, valor_PIS, valor_COFINS, condicao_pagamento, transportadora_pedido, id_transportadora, data_pedido, id_centro_custos, centro_custos_pedido, obs_pedido, obs_interno_pedido, status_pedido, contas_pedido, estoque_pedido, nota_numero, nota_chave, nota_protocolo, nota_data_autorizacao, data_cad_pedido, data_mod_pedido, modelo_nota, serie_nota, importacao, almoxarifado, lixeira, usuario_cad_pedido, usuario_mod_pedido)
                                                        VALUES (@id_entrada, @id_pedido, @id_cliente, @nome_cliente, @vendedor_pedido, @valor_total_produtos, @desconto_pedido, @peso_total_nota, @frete_pedido, @valor_total_nota, @valor_baseICMS, @valor_ICMS, @valor_baseST, @valor_ST, @valor_IPI, @valor_PIS, @valor_COFINS, @condicao_pagamento, @transportadora_pedido, @id_transportadora, @data_pedido, @id_centro_custos, @centro_custos_pedido, @obs_pedido, @obs_interno_pedido, @status_pedido, @contas_pedido, @estoque_pedido, @nota_numero, @nota_chave, @nota_protocolo, @nota_data_autorizacao, COALESCE(NULLIF(@data_cad_pedido, ''), GETDATE()), COALESCE(NULLIF(@data_mod_pedido, ''), GETDATE()), @modelo_nota, @serie_nota, @importacao, @almoxarifado, @lixeira, @usuario_cad_pedido, @usuario_mod_pedido);
                                            ";

                                            // Executar a consulta MERGE na conexão do SQL Server
                                            using (SqlCommand mergeCmd = new SqlCommand(mergeEM1Query, connection))
                                            {
                                                mergeCmd.Parameters.AddWithValue("@id_entrada", id_entrada);
                                                mergeCmd.Parameters.AddWithValue("@id_pedido", id_pedido);
                                                mergeCmd.Parameters.AddWithValue("@id_cliente", id_cliente);
                                                mergeCmd.Parameters.AddWithValue("@nome_cliente", nome_cliente);
                                                mergeCmd.Parameters.AddWithValue("@vendedor_pedido", vendedor_pedido);
                                                mergeCmd.Parameters.AddWithValue("@valor_total_produtos", valor_total_produtos);
                                                mergeCmd.Parameters.AddWithValue("@desconto_pedido", desconto_pedido);
                                                mergeCmd.Parameters.AddWithValue("@peso_total_nota", peso_total_nota);
                                                mergeCmd.Parameters.AddWithValue("@frete_pedido", frete_pedido);
                                                mergeCmd.Parameters.AddWithValue("@valor_total_nota", valor_total_nota);
                                                mergeCmd.Parameters.AddWithValue("@valor_baseICMS", valor_baseICMS);
                                                mergeCmd.Parameters.AddWithValue("@valor_ICMS", valor_ICMS);
                                                mergeCmd.Parameters.AddWithValue("@valor_baseST", valor_baseST);
                                                mergeCmd.Parameters.AddWithValue("@valor_ST", valor_ST);
                                                mergeCmd.Parameters.AddWithValue("@valor_IPI", valor_IPI);
                                                mergeCmd.Parameters.AddWithValue("@valor_PIS", valor_PIS);
                                                mergeCmd.Parameters.AddWithValue("@valor_COFINS", valor_COFINS);
                                                mergeCmd.Parameters.AddWithValue("@condicao_pagamento", condicao_pagamento);
                                                mergeCmd.Parameters.AddWithValue("@transportadora_pedido", transportadora_pedido);
                                                mergeCmd.Parameters.AddWithValue("@id_transportadora", id_transportadora);
                                                mergeCmd.Parameters.AddWithValue("@obs_pedido", obs_pedido);
                                                mergeCmd.Parameters.AddWithValue("@obs_interno_pedido", obs_interno_pedido);
                                                mergeCmd.Parameters.AddWithValue("@status_pedido", status_pedido);
                                                mergeCmd.Parameters.AddWithValue("@contas_pedido", contas_pedido);
                                                mergeCmd.Parameters.AddWithValue("@estoque_pedido", estoque_pedido);
                                                mergeCmd.Parameters.AddWithValue("@data_cad_pedido", data_cad_pedido);
                                                mergeCmd.Parameters.AddWithValue("@data_mod_pedido", DateTime.Now);
                                                mergeCmd.Parameters.AddWithValue("@data_pedido", data_pedido);
                                                mergeCmd.Parameters.AddWithValue("@lixeira", lixeira);
                                                mergeCmd.Parameters.AddWithValue("@nota_numero", nota_numero);
                                                mergeCmd.Parameters.AddWithValue("@nota_chave", nota_chave);
                                                mergeCmd.Parameters.AddWithValue("@nota_protocolo", nota_protocolo);
                                                mergeCmd.Parameters.AddWithValue("@nota_data_autorizacao", nota_data_autorizacao);
                                                mergeCmd.Parameters.AddWithValue("@id_centro_custos", id_centro_custos);
                                                mergeCmd.Parameters.AddWithValue("@centro_custos_pedido", centro_custos_pedido);
                                                mergeCmd.Parameters.AddWithValue("@modelo_nota", modelo_nota);
                                                mergeCmd.Parameters.AddWithValue("@serie_nota", serie_nota);
                                                mergeCmd.Parameters.AddWithValue("@importacao", importacao);
                                                mergeCmd.Parameters.AddWithValue("@almoxarifado", almoxarifado);
                                                mergeCmd.Parameters.AddWithValue("@usuario_cad_pedido", usuario_cad_pedido);
                                                mergeCmd.Parameters.AddWithValue("@usuario_mod_pedido", usuario_mod_pedido);

                                                await mergeCmd.ExecuteNonQueryAsync();
                                                Console.WriteLine("Entrada de mercadoria inserida/atualizada com sucesso na tabela EM1!");
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
                                        Console.WriteLine($"Erro ao processar entrada: {ex.Message}");
                                        Console.WriteLine($"Campo problemático: {IdentificarCampoProblematico(entrada)}");
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
                AtualizarDataUltimaAtualizacaoEM1(connection, "ultima_atualizacao_EM1");
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

    static string IdentificarCampoProblematico(JToken entrada)
    {
        foreach (var property in entrada.Children())
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
    static DateTime ObterDataUltimaAtualizacaoEM1(SqlConnection connection, string campo)
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
    static void AtualizarDataUltimaAtualizacaoEM1(SqlConnection connection, string campo)
    {
        DateTime dataUltimaAtualizacaoEM1 = DateTime.Now; // Defina a data e hora da atualização

        using (SqlCommand command = connection.CreateCommand())
        {
            // Atualize a coluna apropriada com a data e hora da atualização
            command.CommandText = $"UPDATE CTRJOBS SET {campo} = @dataUltimaAtualizacaoEM1";
            command.Parameters.AddWithValue("@dataUltimaAtualizacaoEM1", dataUltimaAtualizacaoEM1);
            command.ExecuteNonQuery();
        }
    }
}