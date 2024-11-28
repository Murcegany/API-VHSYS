using System;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Threading;

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

                string apiAddress = "https://api.vhsys.com/v2/pedidos";
                string accessToken = "token";
                string secretAccessToken = "secret";

                int pageSize = 250;
                int offset = 0;
                // pedidos até 1 dia de atraso
                //DateTime dataLimite = DateTime.Now.AddDays(-1);
                //pedidos hoje 
                DateTime dataLimite = DateTime.Today;
                DateTime dataUltimaAtualizacaoP1 = ObterDataUltimaAtualizacaoP1(connection, "ultima_atualizacao_P1");

                while (true)
                {
                    using (HttpClient httpClient = new HttpClient())
                    {
                        httpClient.DefaultRequestHeaders.Add("Access-Token", accessToken);
                        httpClient.DefaultRequestHeaders.Add("Secret-Access-Token", secretAccessToken);

                        HttpResponseMessage response = await httpClient.GetAsync($"https://api.vhsys.com/v2/pedidos?offset={offset}&limit={pageSize}&order=data_mod_pedido&sort=Desc");

                        if (response.IsSuccessStatusCode)
                        {
                            string jsonResponse = await response.Content.ReadAsStringAsync();
                            JObject data = JObject.Parse(jsonResponse);

                            if (data["data"] is JArray dataArray)
                            {
                                foreach (var pedido in dataArray)
                                {
                                    try
                                    {

                                        int id_ped = GetIntOrNull(pedido, "id_ped") ?? 0;
                                        int id_pedido = GetIntOrNull(pedido, "id_pedido") ?? 0;
                                        int id_cliente = GetIntOrNull(pedido, "id_cliente") ?? 0;
                                        string nome_cliente = GetStringOrNull(pedido, "nome_cliente");
                                        string vendedor_pedido = GetStringOrNull(pedido, "vendedor_pedido");
                                        int vendedor_pedido_id = GetIntOrNull(pedido, "vendedor_pedido_id") ?? 0;
                                        decimal valor_total_produtos = GetDecimalOrNull(pedido, "valor_total_produtos") ?? 0.00M;
                                        decimal desconto_pedido = GetDecimalOrNull(pedido, "desconto_pedido") ?? 0.00M;
                                        decimal peso_total_nota = GetDecimalOrNull(pedido, "peso_total_nota") ?? 0.00M;
                                        decimal frete_pedido = GetDecimalOrNull(pedido, "frete_pedido") ?? 0.00M;
                                        decimal valor_total_nota = GetDecimalOrNull(pedido, "valor_total_nota") ?? 0.00M;
                                        decimal valor_baseICMS = GetDecimalOrNull(pedido, "valor_baseICMS") ?? 0.00M;
                                        decimal valor_ICMS = GetDecimalOrNull(pedido, "valor_ICMS") ?? 0.00M;
                                        decimal valor_baseST = GetDecimalOrNull(pedido, "valor_baseST") ?? 0.00M;
                                        decimal valor_ST = GetDecimalOrNull(pedido, "valor_ST") ?? 0.00M;
                                        decimal valor_IPI = GetDecimalOrNull(pedido, "valor_IPI") ?? 0.00M;
                                        string transportadora_pedido = GetStringOrNull(pedido, "transportadora_pedido");
                                        int id_transportadora = GetIntOrNull(pedido, "id_transportadora") ?? 0;
                                        string prazo_entrega = GetStringOrNull(pedido, "prazo_entrega");
                                        string referencia_pedido = GetStringOrNull(pedido, "referencia_pedido");
                                        string obs_pedido = GetStringOrNull(pedido, "obs_pedido");
                                        string obs_interno_pedido = GetStringOrNull(pedido, "obs_interno_pedido");
                                        string status_pedido = GetStringOrNull(pedido, "status_pedido");
                                        int contas_pedido = GetIntOrNull(pedido, "contas_pedido") ?? 0;
                                        int estoque_pedido = GetIntOrNull(pedido, "estoque_pedido") ?? 0;
                                        int comissao_pedido = GetIntOrNull(pedido, "comissao_pedido") ?? 0;
                                        int ordemc_emitido = GetIntOrNull(pedido, "ordemc_emitido") ?? 0;
                                        DateTime? data_cad_pedido = GetDateTimeOrNull(pedido, "data_cad_pedido");
                                        DateTime? data_pedido = GetDateTimeOrNull(pedido, "data_pedido");
                                        bool lixeira = GetBoolean(pedido, "lixeira");

                                        if (data_mod_pedido >= dataLimite)
                                        {
                                            using (SqlCommand command = connection.CreateCommand())
                                            {

                                                // Construa a consulta MERGE para a tabela P1
                                                string mergeCR1Query = @"
                                                    MERGE INTO P1 AS target
                                                    USING (SELECT @id_ped AS id_ped) AS source
                                                    ON (target.id_ped = source.id_ped)
                                                    WHEN MATCHED THEN
                                                    UPDATE SET
                                                        target.id_pedido = @id_pedido,
                                                        target.id_cliente = @id_cliente,
                                                        target.nome_cliente = @nome_cliente,
                                                        target.vendedor_pedido = @vendedor_pedido,
                                                        target.vendedor_pedido_id = @vendedor_pedido_id,
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
                                                        target.transportadora_pedido = @transportadora_pedido,
                                                        target.id_transportadora = @id_transportadora,
                                                        target.prazo_entrega = @prazo_entrega,
                                                        target.referencia_pedido = @referencia_pedido,
                                                        target.obs_pedido = @obs_pedido,
                                                        target.obs_interno_pedido = @obs_interno_pedido,
                                                        target.status_pedido = @status_pedido,
                                                        target.contas_pedido = @contas_pedido,
                                                        target.estoque_pedido = @estoque_pedido,
                                                        target.comissao_pedido = @comissao_pedido,
                                                        target.ordemc_emitido = @ordemc_emitido,
                                                        target.data_cad_pedido = @data_cad_pedido,
                                                        target.data_mod_pedido = COALESCE(NULLIF(@data_mod_pedido, ''), GETDATE()),
                                                        target.data_pedido = COALESCE(NULLIF(@data_pedido, ''), GETDATE()),
                                                        target.lixeira = @lixeira
                                                    WHEN NOT MATCHED THEN
                                                    INSERT (id_ped, id_pedido, id_cliente, nome_cliente, vendedor_pedido, vendedor_pedido_id, valor_total_produtos, desconto_pedido, peso_total_nota, frete_pedido, valor_total_nota, valor_baseICMS, valor_ICMS, valor_baseST, valor_ST, valor_IPI, transportadora_pedido, id_transportadora, prazo_entrega, referencia_pedido, obs_pedido, obs_interno_pedido, status_pedido, contas_pedido, estoque_pedido, comissao_pedido, ordemc_emitido, data_cad_pedido, data_mod_pedido, data_pedido, lixeira)
                                                    VALUES (@id_ped, @id_pedido, @id_cliente, @nome_cliente, @vendedor_pedido, @vendedor_pedido_id, @valor_total_produtos, @desconto_pedido, @peso_total_nota, @frete_pedido, @valor_total_nota, @valor_baseICMS, @valor_ICMS, @valor_baseST, @valor_ST, @valor_IPI, @transportadora_pedido, @id_transportadora, @prazo_entrega, @referencia_pedido, @obs_pedido, @obs_interno_pedido, @status_pedido, @contas_pedido, @estoque_pedido, @comissao_pedido, @ordemc_emitido, @data_cad_pedido, COALESCE(NULLIF(@data_mod_pedido, ''), GETDATE()), COALESCE(NULLIF(@data_pedido, ''), GETDATE()), @lixeira);
                                                ";

                                                // Executar a consulta MERGE na conexão do SQL Server
                                                using (SqlCommand mergeCmd = new SqlCommand(mergeCR1Query, connection))
                                                {
                                                mergeCmd.Parameters.AddWithValue("@id_ped", id_ped);
                                                mergeCmd.Parameters.AddWithValue("@id_pedido", id_pedido);
                                                mergeCmd.Parameters.AddWithValue("@id_cliente", id_cliente);
                                                mergeCmd.Parameters.AddWithValue("@nome_cliente", nome_cliente);
                                                mergeCmd.Parameters.AddWithValue("@vendedor_pedido", vendedor_pedido);
                                                mergeCmd.Parameters.AddWithValue("@vendedor_pedido_id", vendedor_pedido_id);
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
                                                mergeCmd.Parameters.AddWithValue("@transportadora_pedido", transportadora_pedido);
                                                mergeCmd.Parameters.AddWithValue("@id_transportadora", id_transportadora);
                                                mergeCmd.Parameters.AddWithValue("@prazo_entrega", prazo_entrega);
                                                mergeCmd.Parameters.AddWithValue("@referencia_pedido", referencia_pedido);
                                                mergeCmd.Parameters.AddWithValue("@obs_pedido", obs_pedido);
                                                mergeCmd.Parameters.AddWithValue("@obs_interno_pedido", obs_interno_pedido);
                                                mergeCmd.Parameters.AddWithValue("@status_pedido", status_pedido);
                                                mergeCmd.Parameters.AddWithValue("@contas_pedido", contas_pedido);
                                                mergeCmd.Parameters.AddWithValue("@estoque_pedido", estoque_pedido);
                                                mergeCmd.Parameters.AddWithValue("@comissao_pedido", comissao_pedido);
                                                mergeCmd.Parameters.AddWithValue("@ordemc_emitido", ordemc_emitido);
                                                mergeCmd.Parameters.AddWithValue("@data_cad_pedido", data_cad_pedido);
                                                mergeCmd.Parameters.AddWithValue("@data_mod_pedido", data_mod_pedido);
                                                mergeCmd.Parameters.AddWithValue("@data_pedido", data_pedido);
                                                mergeCmd.Parameters.AddWithValue("@lixeira", lixeira);

                                                    // linha utilizada para teste de resposta
                                                    foreach (SqlParameter parameter in mergeCmd.Parameters)
                                                    {
                                                        Console.WriteLine($"{parameter.ParameterName}: {parameter.Value}");
                                                    }

                                                    await mergeCmd.ExecuteNonQueryAsync();
                                                    Console.WriteLine("Pedido inserida/atualizada com sucesso na tabela CR1!");
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
                                        Console.WriteLine($"Campo problemático: {IdentificarCampoProblematico(pedido)}");
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

                AtualizarDataUltimaAtualizacaoP1(connection, "ultima_atualizacao_P1");
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
    static DateTime ObterDataUltimaAtualizacaoP1(SqlConnection connection, string campo)
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
    static void AtualizarDataUltimaAtualizacaoP1(SqlConnection connection, string campo)
    {
        DateTime dataUltimaAtualizacaoP1 = DateTime.Now; // Defina a data e hora da atualização

        using (SqlCommand command = connection.CreateCommand())
        {
            // Atualize a coluna apropriada com a data e hora da atualização
            command.CommandText = $"UPDATE CTRJOBS SET {campo} = @dataUltimaAtualizacaoP1";
            command.Parameters.AddWithValue("@dataUltimaAtualizacaoP1", dataUltimaAtualizacaoP1);
            command.ExecuteNonQuery();
        }
    }
}

