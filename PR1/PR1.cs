using System;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

public class PR1
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

                string apiAddress = "https://api.vhsys.com/v2/produtos";
                string accessToken = "token";
                string secretAccessToken = "secret";
                DateTime dataLimite = DateTime.Today;

                int pageSize = 250; // Tamanho da página
                int offset = 0;

                // Loop para obter os dados da API em lotes de páginas
                while (true)
                {
                    using (HttpClient httpClient = new HttpClient())
                    {
                        // Adicionar os cabeçalhos à solicitação HTTP
                        httpClient.DefaultRequestHeaders.Add("Access-Token", accessToken);
                        httpClient.DefaultRequestHeaders.Add("Secret-Access-Token", secretAccessToken);

                        string requestUrl = $"{apiAddress}?limit={pageSize}&offset={offset}&sort=Desc&order=data_mod_produto";
                        HttpResponseMessage response = await httpClient.GetAsync(requestUrl);

                        if (response.IsSuccessStatusCode)
                        {
                            string jsonResponse = await response.Content.ReadAsStringAsync();
                            JObject data = JObject.Parse(jsonResponse);

                            if (data["data"] is JArray dataArray)
                            {
                                foreach (var produto in dataArray)
                                {
                                    var data_mod_produto = produto["data_mod_produto"];

                                    if (data_mod_produto != null && DateTime.TryParse(data_mod_produto.ToString(), out DateTime data_mod_produtoValue))
                                    {
                                        if (data_mod_produtoValue >= dataLimite)
                                        {
                                            using (SqlCommand command = connection.CreateCommand())
                                            {
                                                command.CommandText = @"
                                                MERGE INTO PR1 AS target
                                                USING (SELECT @id_produto AS id_produto) AS source
                                                ON (target.id_produto = source.id_produto)
                                                WHEN MATCHED THEN
                                                    UPDATE SET
                                                        target.id_registro = @id_registro,
                                                        target.id_categoria = @id_categoria,
                                                        target.cod_produto = @cod_produto,
                                                        target.marca_produto = @marca_produto,
                                                        target.desc_produto = @desc_produto,
                                                        target.atalho_produto = @atalho_produto,
                                                        target.fornecedor_produto = @fornecedor_produto,
                                                        target.fornecedor_produto_id = @fornecedor_produto_id,
                                                        target.minimo_produto = @minimo_produto,
                                                        target.maximo_produto = @maximo_produto,
                                                        target.estoque_produto = @estoque_produto,
                                                        target.unidade_produto = @unidade_produto,
                                                        target.valor_produto = @valor_produto,
                                                        target.valor_custo_produto = @valor_custo_produto,
                                                        target.peso_produto = @peso_produto,
                                                        target.peso_liq_produto = @peso_liq_produto,
                                                        target.icms_produto = @icms_produto,
                                                        target.ipi_produto = @ipi_produto,
                                                        target.pis_produto = @pis_produto,
                                                        target.cofins_produto = @cofins_produto,
                                                        target.unidade_tributavel = @unidade_tributavel,
                                                        target.cest_produto = @cest_produto,
                                                        target.ncm_produto = @ncm_produto,
                                                        target.codigo_barra_produto = @codigo_barra_produto,
                                                        target.obs_produto = @obs_produto,
                                                        target.tipo_produto = @tipo_produto,
                                                        target.tamanho_produto = @tamanho_produto,
                                                        target.localizacao_produto = @localizacao_produto,
                                                        target.kit_produto = @kit_produto,
                                                        target.status_produto = @status_produto,
                                                        target.data_cad_produto = @data_cad_produto,
                                                        target.data_mod_produto = @data_mod_produto,
                                                        target.lixeira = @lixeira
                                                WHEN NOT MATCHED THEN
                                                    INSERT (
                                                        id_produto,
                                                        id_registro,
                                                        id_categoria,
                                                        cod_produto,
                                                        marca_produto,
                                                        desc_produto,
                                                        atalho_produto,
                                                        fornecedor_produto,
                                                        fornecedor_produto_id,
                                                        minimo_produto,
                                                        maximo_produto,
                                                        estoque_produto,
                                                        unidade_produto,
                                                        valor_produto,
                                                        valor_custo_produto,
                                                        peso_produto,
                                                        peso_liq_produto,
                                                        icms_produto,
                                                        ipi_produto,
                                                        pis_produto,
                                                        cofins_produto,
                                                        unidade_tributavel,
                                                        cest_produto,
                                                        ncm_produto,
                                                        codigo_barra_produto,
                                                        obs_produto,
                                                        tipo_produto,
                                                        tamanho_produto,
                                                        localizacao_produto,
                                                        kit_produto,
                                                        status_produto,
                                                        data_cad_produto,
                                                        data_mod_produto,
                                                        lixeira
                                                    )
                                                    VALUES (
                                                        @id_produto,
                                                        @id_registro,
                                                        @id_categoria,
                                                        @cod_produto,
                                                        @marca_produto,
                                                        @desc_produto,
                                                        @atalho_produto,
                                                        @fornecedor_produto,
                                                        @fornecedor_produto_id,
                                                        @minimo_produto,
                                                        @maximo_produto,
                                                        @estoque_produto,
                                                        @unidade_produto,
                                                        @valor_produto,
                                                        @valor_custo_produto,
                                                        @peso_produto,
                                                        @peso_liq_produto,
                                                        @icms_produto,
                                                        @ipi_produto,
                                                        @pis_produto,
                                                        @cofins_produto,
                                                        @unidade_tributavel,
                                                        @cest_produto,
                                                        @ncm_produto,
                                                        @codigo_barra_produto,
                                                        @obs_produto,
                                                        @tipo_produto,
                                                        @tamanho_produto,
                                                        @localizacao_produto,
                                                        @kit_produto,
                                                        @status_produto,
                                                        @data_cad_produto,
                                                        @data_mod_produto,
                                                        @lixeira
                                                    );
                                                ";

                                                // Defina os valores dos parâmetros dentro do loop
                                                command.Parameters.AddWithValue("@id_produto", produto["id_produto"].ToString());
                                                command.Parameters.AddWithValue("@id_registro", produto["id_registro"].ToString());
                                                command.Parameters.AddWithValue("@id_categoria", produto["id_categoria"].ToString());
                                                command.Parameters.AddWithValue("@cod_produto", produto["cod_produto"].ToString());
                                                command.Parameters.AddWithValue("@marca_produto", produto["marca_produto"].ToString());
                                                command.Parameters.AddWithValue("@desc_produto", produto["desc_produto"].ToString());
                                                command.Parameters.AddWithValue("@atalho_produto", produto["atalho_produto"].ToString());
                                                command.Parameters.AddWithValue("@fornecedor_produto", produto["fornecedor_produto"].ToString());
                                                command.Parameters.AddWithValue("@fornecedor_produto_id", produto["fornecedor_produto_id"].ToString());
                                                command.Parameters.AddWithValue("@unidade_tributavel", produto["unidade_tributavel"].ToString());
                                                command.Parameters.AddWithValue("@cest_produto", produto["cest_produto"].ToString());
                                                command.Parameters.AddWithValue("@ncm_produto", produto["ncm_produto"].ToString());
                                                command.Parameters.AddWithValue("@codigo_barra_produto", produto["codigo_barra_produto"].ToString());
                                                command.Parameters.AddWithValue("@obs_produto", produto["obs_produto"].ToString());
                                                command.Parameters.AddWithValue("@tipo_produto", produto["tipo_produto"].ToString());
                                                command.Parameters.AddWithValue("@tamanho_produto", produto["tamanho_produto"].ToString());
                                                command.Parameters.AddWithValue("@localizacao_produto", produto["localizacao_produto"].ToString());
                                                command.Parameters.AddWithValue("@kit_produto", produto["kit_produto"].ToString());
                                                command.Parameters.AddWithValue("@status_produto", produto["status_produto"].ToString());
                                                command.Parameters.AddWithValue("@unidade_produto", produto["unidade_produto"].ToString());

                                                string minimoProdutoValue = produto["minimo_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(minimoProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@minimo_produto", minimoProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@minimo_produto", DBNull.Value);
                                                }

                                                string maximoProdutoValue = produto["maximo_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(maximoProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@maximo_produto", maximoProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@maximo_produto", DBNull.Value);
                                                }

                                                string estoqueProdutoValue = produto["estoque_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(estoqueProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@estoque_produto", estoqueProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@estoque_produto", DBNull.Value);
                                                }

                                                string valorProdutoValue = produto["valor_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(valorProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@valor_produto", valorProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@valor_produto", DBNull.Value);
                                                }

                                                string valorCustoProdutoValue = produto["valor_custo_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(valorCustoProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@valor_custo_produto", valorCustoProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@valor_custo_produto", DBNull.Value);
                                                }

                                                string pesoProdutoValue = produto["peso_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(pesoProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@peso_produto", pesoProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@peso_produto", DBNull.Value);
                                                }

                                                string pesoLiqProdutoValue = produto["peso_liq_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(pesoLiqProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@peso_liq_produto", pesoLiqProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@peso_liq_produto", DBNull.Value);
                                                }

                                                string icmsProdutoValue = produto["icms_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(icmsProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@icms_produto", icmsProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@icms_produto", DBNull.Value);
                                                }

                                                string ipiProdutoValue = produto["ipi_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(ipiProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@ipi_produto", ipiProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@ipi_produto", DBNull.Value);
                                                }

                                                string cofinsProdutoValue = produto["cofins_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(cofinsProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@cofins_produto", cofinsProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@cofins_produto", DBNull.Value);
                                                }

                                                string pisProdutoValue = produto["pis_produto"]?.ToString();
                                                if (!string.IsNullOrEmpty(pisProdutoValue))
                                                {
                                                    command.Parameters.AddWithValue("@pis_produto", pisProdutoValue);
                                                }
                                                else
                                                {
                                                    command.Parameters.AddWithValue("@pis_produto", DBNull.Value);
                                                }

                                                    string dataCadProdutoValue = produto["data_cad_produto"]?.ToString();
                                                    if (DateTime.TryParse(dataCadProdutoValue, out DateTime dataCadProduto))
                                                    {
                                                        // Formate a data no formato ISO 8601 e insira-a no banco de dados
                                                        string produtoDataCadFormatted = dataCadProduto.ToString("yyyy-MM-ddTHH:mm:ss");
                                                        command.Parameters.AddWithValue("@data_cad_produto", produtoDataCadFormatted);
                                                    }
                                                    else
                                                    {
                                                        command.Parameters.AddWithValue("@data_cad_produto", DBNull.Value);
                                                    }

                                                    string dataModProdutoValue = produto["data_mod_produto"]?.ToString();
                                                    if (DateTime.TryParse(dataModProdutoValue, out DateTime dataModProduto))
                                                    {
                                                        // Formate a data no formato ISO 8601 e insira-a no banco de dados
                                                        string produtoDataModFormatted = dataModProduto.ToString("yyyy-MM-ddTHH:mm:ss");
                                                        command.Parameters.AddWithValue("@data_mod_produto", produtoDataModFormatted);
                                                    }
                                                    else
                                                    {
                                                        command.Parameters.AddWithValue("@data_mod_produto", DBNull.Value);
                                                    }



                                                // Defina o valor de @lixeira com base na condição
                                                command.Parameters.AddWithValue("@lixeira", (produto["lixeira"].ToString().ToLower() == "Nao") ? false : true);
                                                // Após definir os valores dos parâmetros, adicione esta lógica de depuração

                                                foreach (SqlParameter parameter in command.Parameters)
                                                { //linha utilizada para teste de resposta
                                                    Console.WriteLine($"{parameter.ParameterName}: {parameter.Value}");
                                                }

                                                int rowsAffected = command.ExecuteNonQuery();
                                                Console.WriteLine($"Produto inserido com sucesso! Linhas afetadas: {rowsAffected}");
                                            }
                                        }
                                        else
                                        {
                                            Console.WriteLine("Pedidos de hoje inseridos. Parando a execução do código.");
                                            Environment.Exit(0); // Encerra o programa
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        // Trate o caso em que não é possível obter a data_mod_produto
                                        // Defina um valor padrão ou tome a ação apropriada
                                        data_mod_produtoValue = DateTime.MinValue; // Valor padrão, ajuste conforme necessário
                                    }
                                }

                                // Se o número de produtos na página for menor que o tamanho da página,
                                // indica que não há mais páginas a serem recuperadas.
                                if (dataArray.Count < pageSize)
                                {
                                    break; // Sai do loop
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
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro: {ex.Message}");
        }
    }

            // Função para obter a data da última execução da tabela de controle (CTRJOBS)
    static DateTime ObterDataUltimaAtualizacaoPR1(SqlConnection connection, string campo)
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
    static void AtualizarDataUltimaAtualizacaoPR1(SqlConnection connection, string campo)
    {
        DateTime dataUltimaAtualizacaoPR1 = DateTime.Now; // Defina a data e hora da atualização

        using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = $"UPDATE CTRJOBS SET {campo} = @dataUltimaAtualizacaoPR1";
            command.Parameters.AddWithValue("@dataUltimaAtualizacaoPR1", dataUltimaAtualizacaoPR1);
            command.ExecuteNonQuery();
        }
    }
}
