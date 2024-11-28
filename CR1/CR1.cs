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

                string apiAddress = "https://api.vhsys.com/v2/contas-receber";
                string accessToken = "token";
                string secretAccessToken = "secret";

                int pageSize = 250;
                int offset = 0;
                // pedidos até 1 dia de atraso
                //DateTime dataLimite = DateTime.Now.AddDays(-1);
                //pedidos hoje 
                DateTime dataUltimaAtualizacaoCR1 = ObterDataUltimaAtualizacaoCR1(connection, "ultima_atualizacao_CR1");

                while (true)
                {
                    using (HttpClient httpClient = new HttpClient())
                    {
                        httpClient.DefaultRequestHeaders.Add("Access-Token", accessToken);
                        httpClient.DefaultRequestHeaders.Add("Secret-Access-Token", secretAccessToken);

                        HttpResponseMessage response = await httpClient.GetAsync($"https://api.vhsys.com/v2/contas-receber?offset={offset}&limit={pageSize}&order=vencimento_rec&sort=Desc");

                        if (response.IsSuccessStatusCode)
                        {
                            string jsonResponse = await response.Content.ReadAsStringAsync();
                            JObject data = JObject.Parse(jsonResponse);

                            if (data["data"] is JArray dataArray)
                            {
                                foreach (var contaReceber in dataArray)
                                {
                                    try
                                    {
                                        DateTime? data_mod_rec = GetDateTimeOrNull(contaReceber, "data_mod_rec");

                                        // Extrair os dados do pedido da API
                                        int id_conta_rec = GetIntOrNull(contaReceber, "id_conta_rec") ?? 0;
                                        int id_empresa = GetIntOrNull(contaReceber, "id_empresa") ?? 0;
                                        int id_registro = GetIntOrNull(contaReceber, "id_registro") ?? 0;
                                        string identificacao = GetStringOrNull(contaReceber, "identificacao");
                                        string nome_conta = GetStringOrNull(contaReceber, "nome_conta");
                                        int id_categoria = GetIntOrNull(contaReceber, "id_categoria") ?? 0;
                                        string categoria_rec = GetStringOrNull(contaReceber, "categoria_rec");
                                        int id_banco = GetIntOrNull(contaReceber, "id_banco") ?? 0;
                                        int id_cliente = GetIntOrNull(contaReceber, "id_cliente") ?? 0;
                                        string nome_cliente = GetStringOrNull(contaReceber, "nome_cliente");
                                        DateTime? vencimento_rec = GetDateTimeOrNull(contaReceber, "vencimento_rec");
                                        decimal valor_rec = GetDecimalOrNull(contaReceber, "valor_rec") ?? 0.00M;
                                        decimal valor_pago = GetDecimalOrNull(contaReceber, "valor_pago") ?? 0.00M;
                                        DateTime? data_emissao = GetDateTimeOrNull(contaReceber, "data_emissao");
                                        DateTime? vencimento_original = GetDateTimeOrNull(contaReceber, "vencimento_original");
                                        string n_documento_rec = GetStringOrNull(contaReceber, "n_documento_rec");
                                        string observacoes_rec = GetStringOrNull(contaReceber, "observacoes_rec");
                                        int id_centro_custos = GetIntOrNull(contaReceber, "id_centro_custos") ?? 0;
                                        string centro_custos_rec = GetStringOrNull(contaReceber, "centro_custos_rec");
                                        string praca_pagamento = GetStringOrNull(contaReceber, "praca_pagamento");
                                        string liquidado_rec = GetStringOrNull(contaReceber, "liquidado_rec");
                                        DateTime? data_pagamento = GetDateTimeOrNull(contaReceber, "data_pagamento");
                                        string obs_pagamento = GetStringOrNull(contaReceber, "obs_pagamento");
                                        string forma_pagamento = GetStringOrNull(contaReceber, "forma_pagamento");
                                        decimal valor_juros = GetDecimalOrNull(contaReceber, "valor_juros") ?? 0.00M;
                                        decimal valor_desconto = GetDecimalOrNull(contaReceber, "valor_desconto") ?? 0.00M;
                                        decimal valor_acrescimo = GetDecimalOrNull(contaReceber, "valor_acrescimo") ?? 0.00M;
                                        decimal valor_taxa = GetDecimalOrNull(contaReceber, "valor_taxa") ?? 0.00M;
                                        int retorno_pagamento = GetIntOrNull(contaReceber, "retorno_pagamento") ?? 0;
                                        string tipo_conta = GetStringOrNull(contaReceber, "tipo_conta");
                                        DateTime? data_cad_rec = GetDateTimeOrNull(contaReceber, "data_cad_rec");
                                        int boleto_enviado = GetIntOrNull(contaReceber, "boleto_enviado") ?? 0;
                                        int boleto_original = GetIntOrNull(contaReceber, "boleto_original") ?? 0;
                                        int duplicata_enviado = GetIntOrNull(contaReceber, "duplicata_enviado") ?? 0;
                                        int remetido = GetIntOrNull(contaReceber, "remetido") ?? 0;
                                        int registrado = GetIntOrNull(contaReceber, "registrado") ?? 0;
                                        int protestar = GetIntOrNull(contaReceber, "protestar") ?? 0;
                                        int dias_protestar = GetIntOrNull(contaReceber, "dias_protestar") ?? 0;
                                        string NossoNumero = GetStringOrNull(contaReceber, "NossoNumero");
                                        int agrupado = GetIntOrNull(contaReceber, "agrupado") ?? 0;
                                        int agrupado_data = GetIntOrNull(contaReceber, "agrupado_data") ?? 0;
                                        int agrupado_user = GetIntOrNull(contaReceber, "agrupado_user") ?? 0;
                                        int agrupamento = GetIntOrNull(contaReceber, "agrupamento") ?? 0;
                                        int fluxo = GetIntOrNull(contaReceber, "fluxo") ?? 0;
                                        string lixeira = GetStringOrNull(contaReceber, "lixeira");
                                        int id_pagamento_ob = GetIntOrNull(contaReceber, "id_pagamento_ob") ?? 0;
                                        string situacao = GetStringOrNull(contaReceber, "situacao");
                                        int status = GetIntOrNull(contaReceber, "status") ?? 0;
                                        decimal valor_baixa = GetDecimalOrNull(contaReceber, "valor_baixa") ?? 0.00M;
                                        string link_boleto = GetStringOrNull(contaReceber, "link_boleto");
                                            using (SqlCommand command = connection.CreateCommand())
                                            {

                                                // Construa a consulta MERGE para a tabela P1
                                                string mergeCR1Query = @"
                                                    MERGE INTO CR1 AS target
                                                    USING (SELECT @id_conta_rec AS id_conta_rec) AS source
                                                    ON (target.id_conta_rec = source.id_conta_rec)
                                                    WHEN MATCHED THEN
                                                        UPDATE SET
                                                            target.id_empresa = @id_empresa,
                                                            target.id_registro = @id_registro,
                                                            target.identificacao = @identificacao,
                                                            target.nome_conta = @nome_conta,
                                                            target.id_categoria = @id_categoria,
                                                            target.categoria_rec = @categoria_rec,
                                                            target.id_banco = @id_banco,
                                                            target.id_cliente = @id_cliente,
                                                            target.nome_cliente = @nome_cliente,
                                                            target.vencimento_rec = @vencimento_rec,
                                                            target.valor_rec = @valor_rec,
                                                            target.valor_pago = @valor_pago,
                                                            target.data_emissao = @data_emissao,
                                                            target.vencimento_original = @vencimento_original,
                                                            target.n_documento_rec = @n_documento_rec,
                                                            target.observacoes_rec = @observacoes_rec,
                                                            target.id_centro_custos = @id_centro_custos,
                                                            target.centro_custos_rec = @centro_custos_rec,
                                                            target.praca_pagamento = @praca_pagamento,
                                                            target.liquidado_rec = @liquidado_rec,
                                                            target.data_pagamento = @data_pagamento,
                                                            target.obs_pagamento = @obs_pagamento,
                                                            target.forma_pagamento = @forma_pagamento,
                                                            target.valor_juros = @valor_juros,
                                                            target.valor_desconto = @valor_desconto,
                                                            target.valor_acrescimo = @valor_acrescimo,
                                                            target.valor_taxa = @valor_taxa,
                                                            target.retorno_pagamento = @retorno_pagamento,
                                                            target.tipo_conta = @tipo_conta,
                                                            target.data_cad_rec = @data_cad_rec,
                                                            target.data_mod_rec = @data_mod_rec,
                                                            target.boleto_enviado = @boleto_enviado,
                                                            target.boleto_original = @boleto_original,
                                                            target.duplicata_enviado = @duplicata_enviado,
                                                            target.remetido = @remetido,
                                                            target.registrado = @registrado,
                                                            target.protestar = @protestar,
                                                            target.dias_protestar = @dias_protestar,
                                                            target.NossoNumero = @NossoNumero,
                                                            target.agrupado = @agrupado,
                                                            target.agrupado_data = @agrupado_data,
                                                            target.agrupado_user = @agrupado_user,
                                                            target.agrupamento = @agrupamento,
                                                            target.fluxo = @fluxo,
                                                            target.lixeira = @lixeira,
                                                            target.id_pagamento_ob = @id_pagamento_ob,
                                                            target.situacao = @situacao,
                                                            target.status = @status,
                                                            target.valor_baixa = @valor_baixa,
                                                            target.link_boleto = @link_boleto
                                                    WHEN NOT MATCHED THEN
                                                        INSERT (id_conta_rec, id_empresa, id_registro, identificacao, nome_conta, id_categoria, categoria_rec, id_banco, id_cliente, nome_cliente, vencimento_rec, valor_rec, valor_pago, data_emissao, vencimento_original, n_documento_rec, observacoes_rec, id_centro_custos, centro_custos_rec, praca_pagamento, liquidado_rec, data_pagamento, obs_pagamento, forma_pagamento, valor_juros, valor_desconto, valor_acrescimo, valor_taxa, retorno_pagamento, tipo_conta, data_cad_rec, data_mod_rec, boleto_enviado, boleto_original, duplicata_enviado, remetido, registrado, protestar, dias_protestar, NossoNumero, agrupado, agrupado_data, agrupado_user, agrupamento, fluxo, lixeira, id_pagamento_ob, situacao, status, valor_baixa, link_boleto)
                                                        VALUES (@id_conta_rec, @id_empresa, @id_registro, @identificacao, @nome_conta, @id_categoria, @categoria_rec, @id_banco, @id_cliente, @nome_cliente, @vencimento_rec, @valor_rec, @valor_pago, @data_emissao, @vencimento_original, @n_documento_rec, @observacoes_rec, @id_centro_custos, @centro_custos_rec, @praca_pagamento, @liquidado_rec, @data_pagamento, @obs_pagamento, @forma_pagamento, @valor_juros, @valor_desconto, @valor_acrescimo, @valor_taxa, @retorno_pagamento, @tipo_conta, @data_cad_rec, @data_mod_rec, @boleto_enviado, @boleto_original, @duplicata_enviado, @remetido, @registrado, @protestar, @dias_protestar, @NossoNumero, @agrupado, @agrupado_data, @agrupado_user, @agrupamento, @fluxo, @lixeira, @id_pagamento_ob, @situacao, @status, @valor_baixa, @link_boleto);
                                                ";

                                                // Executar a consulta MERGE na conexão do SQL Server
                                                using (SqlCommand mergeCmd = new SqlCommand(mergeCR1Query, connection))
                                                {
                                                    mergeCmd.Parameters.AddWithValue("@id_conta_rec", id_conta_rec);
                                                    mergeCmd.Parameters.AddWithValue("@id_empresa", id_empresa);
                                                    mergeCmd.Parameters.AddWithValue("@id_registro", id_registro);
                                                    mergeCmd.Parameters.AddWithValue("@identificacao", identificacao);
                                                    mergeCmd.Parameters.AddWithValue("@vencimento_original", (object)vencimento_original ?? DBNull.Value);
                                                    mergeCmd.Parameters.AddWithValue("@nome_conta", nome_conta);
                                                    mergeCmd.Parameters.AddWithValue("@id_categoria", id_categoria);
                                                    mergeCmd.Parameters.AddWithValue("@categoria_rec", categoria_rec);
                                                    mergeCmd.Parameters.AddWithValue("@id_banco", id_banco);
                                                    mergeCmd.Parameters.AddWithValue("@id_cliente", id_cliente);
                                                    mergeCmd.Parameters.AddWithValue("@nome_cliente", nome_cliente);
                                                    mergeCmd.Parameters.AddWithValue("@vencimento_rec", vencimento_rec);
                                                    mergeCmd.Parameters.AddWithValue("@valor_rec", valor_rec);
                                                    mergeCmd.Parameters.AddWithValue("@valor_pago", valor_pago);
                                                    mergeCmd.Parameters.AddWithValue("@data_emissao", data_emissao);
                                                    mergeCmd.Parameters.AddWithValue("@n_documento_rec", n_documento_rec);
                                                    mergeCmd.Parameters.AddWithValue("@observacoes_rec", observacoes_rec);
                                                    mergeCmd.Parameters.AddWithValue("@id_centro_custos", id_centro_custos);
                                                    mergeCmd.Parameters.AddWithValue("@centro_custos_rec", centro_custos_rec);
                                                    mergeCmd.Parameters.AddWithValue("@praca_pagamento", praca_pagamento);
                                                    mergeCmd.Parameters.AddWithValue("@liquidado_rec", liquidado_rec);
                                                    mergeCmd.Parameters.AddWithValue("@data_pagamento", (object)data_pagamento ?? DBNull.Value); // Permitir que seja nulo
                                                    mergeCmd.Parameters.AddWithValue("@obs_pagamento", obs_pagamento);
                                                    mergeCmd.Parameters.AddWithValue("@forma_pagamento", forma_pagamento);
                                                    mergeCmd.Parameters.AddWithValue("@valor_juros", valor_juros);
                                                    mergeCmd.Parameters.AddWithValue("@valor_desconto", valor_desconto);
                                                    mergeCmd.Parameters.AddWithValue("@valor_acrescimo", valor_acrescimo);
                                                    mergeCmd.Parameters.AddWithValue("@valor_taxa", valor_taxa);
                                                    mergeCmd.Parameters.AddWithValue("@retorno_pagamento", retorno_pagamento);
                                                    mergeCmd.Parameters.AddWithValue("@tipo_conta", tipo_conta);
                                                    mergeCmd.Parameters.AddWithValue("@data_cad_rec", data_cad_rec);
                                                    mergeCmd.Parameters.AddWithValue("@data_mod_rec", data_mod_rec);
                                                    mergeCmd.Parameters.AddWithValue("@boleto_enviado", boleto_enviado);
                                                    mergeCmd.Parameters.AddWithValue("@boleto_original", boleto_original);
                                                    mergeCmd.Parameters.AddWithValue("@duplicata_enviado", duplicata_enviado);
                                                    mergeCmd.Parameters.AddWithValue("@remetido", remetido);
                                                    mergeCmd.Parameters.AddWithValue("@registrado", registrado);
                                                    mergeCmd.Parameters.AddWithValue("@protestar", protestar);
                                                    mergeCmd.Parameters.AddWithValue("@dias_protestar", dias_protestar);
                                                    mergeCmd.Parameters.AddWithValue("@NossoNumero", NossoNumero);
                                                    mergeCmd.Parameters.AddWithValue("@agrupado", agrupado);
                                                    mergeCmd.Parameters.AddWithValue("@agrupado_data", agrupado_data);
                                                    mergeCmd.Parameters.AddWithValue("@agrupado_user", agrupado_user);
                                                    mergeCmd.Parameters.AddWithValue("@agrupamento", agrupamento);
                                                    mergeCmd.Parameters.AddWithValue("@fluxo", fluxo);
                                                    mergeCmd.Parameters.AddWithValue("@lixeira", lixeira);
                                                    mergeCmd.Parameters.AddWithValue("@id_pagamento_ob", id_pagamento_ob);
                                                    mergeCmd.Parameters.AddWithValue("@situacao", situacao);
                                                    mergeCmd.Parameters.AddWithValue("@status", status);
                                                    mergeCmd.Parameters.AddWithValue("@valor_baixa", valor_baixa);
                                                    mergeCmd.Parameters.AddWithValue("@link_boleto", link_boleto);

                                                    // linha utilizada para teste de resposta
                                                    foreach (SqlParameter parameter in mergeCmd.Parameters)
                                                    {
                                                        Console.WriteLine($"{parameter.ParameterName}: {parameter.Value}");
                                                    }

                                                    await mergeCmd.ExecuteNonQueryAsync();
                                                    Console.WriteLine("Conta inserida/atualizada com sucesso na tabela CR1!");
                                                }
                                            }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Erro ao processar pedido: {ex.Message}");
                                        Console.WriteLine($"Campo problemático: {IdentificarCampoProblematico(contaReceber)}");
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

                AtualizarDataUltimaAtualizacaoCR1(connection, "ultima_atualizacao_CR1");
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
    static DateTime ObterDataUltimaAtualizacaoCR1(SqlConnection connection, string campo)
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
    static void AtualizarDataUltimaAtualizacaoCR1(SqlConnection connection, string campo)
    {
        DateTime dataUltimaAtualizacaoCR1 = DateTime.Now; // Defina a data e hora da atualização

        using (SqlCommand command = connection.CreateCommand())
        {
            // Atualize a coluna apropriada com a data e hora da atualização
            command.CommandText = $"UPDATE CTRJOBS SET {campo} = @dataUltimaAtualizacaoCR1";
            command.Parameters.AddWithValue("@dataUltimaAtualizacaoCR1", dataUltimaAtualizacaoCR1);
            command.ExecuteNonQuery();
        }
    }
}

