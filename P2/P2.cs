using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

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

                string accessToken = "token";
                string secretAccessToken = "secret";
                int pageSize = 250;
                int offset = 0;

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

                            if (data["data"] is JArray pedidosArray)
                            {
                                int numberOfPedidos = pedidosArray.Count;
                                Console.WriteLine($"Número de pedidos na página offset {offset}: {numberOfPedidos}");

                                bool hasTodayOrders = false;

                                foreach (var pedido in pedidosArray)
                                {
                                    DateTime? dataModPedido = GetDateTimeOrNull(pedido, "data_mod_pedido");

                                    if (dataModPedido.HasValue && dataModPedido.Value.Date == DateTime.Today)
                                    {
                                        hasTodayOrders = true;

                                        int idPedido = (int)pedido["id_ped"];

                                        string url = $"https://api.vhsys.com/v2/pedidos/{idPedido}/produtos";
                                        HttpResponseMessage produtosResponse = await httpClient.GetAsync(url);

                                        if (produtosResponse.IsSuccessStatusCode)
                                        {
                                            string produtosJsonResponse = await produtosResponse.Content.ReadAsStringAsync();
                                            JObject produtos = JObject.Parse(produtosJsonResponse);

                                            if (produtos["data"] is JArray produtosArray)
                                            {
                                                foreach (var produto in produtosArray)
                                                {
                                                int idPedProduto = (int)produto["id_ped_produto"];
                                                int idPedidoProduto = (int)produto["id_pedido"];
                                                int idProduto = (int)produto["id_produto"];
                                                string descricaoProduto = (string)produto["desc_produto"];
                                                decimal qtdeProduto = (decimal)produto["qtde_produto"];
                                                decimal descontoProduto = (decimal)produto["desconto_produto"];
                                                decimal ipiProduto = (decimal)produto["ipi_produto"];
                                                decimal icmsProduto = (decimal)produto["icms_produto"];
                                                decimal valorUnitProduto = (decimal)produto["valor_unit_produto"];
                                                decimal valorCustoProduto = (decimal)produto["valor_custo_produto"];
                                                decimal valorTotalProduto = (decimal)produto["valor_total_produto"];
                                                decimal pesoProduto = (decimal)produto["peso_produto"];
                                                decimal pesoLiqProduto = (decimal)produto["peso_liq_produto"];

                                                string insertOrUpdateQuery = @"
                                                    MERGE INTO P2 AS target
                                                    USING (SELECT @idPedProduto AS id_ped_produto) AS source
                                                    ON (target.id_ped_produto = source.id_ped_produto)
                                                    WHEN MATCHED THEN
                                                    UPDATE SET
                                                        target.id_pedido = @idPedidoProduto,
                                                        target.id_produto = @idProduto,
                                                        target.descricao = @descricaoProduto,
                                                        target.qtde_produto = @qtdeProduto,
                                                        target.desconto_produto = @descontoProduto,
                                                        target.ipi_produto = @ipiProduto,
                                                        target.icms_produto = @icmsProduto,
                                                        target.valor_unit_produto = @valorUnitProduto,
                                                        target.valor_custo_produto = @valorCustoProduto,
                                                        target.valor_total_produto = @valorTotalProduto,
                                                        target.peso_produto = @pesoProduto,
                                                        target.peso_liq_produto = @pesoLiqProduto
                                                    WHEN NOT MATCHED THEN
                                                    INSERT (id_ped_produto, id_pedido, id_produto, descricao, qtde_produto, desconto_produto, ipi_produto, icms_produto, valor_unit_produto, valor_custo_produto, valor_total_produto, peso_produto, peso_liq_produto)
                                                    VALUES (@idPedProduto, @idPedidoProduto, @idProduto, @descricaoProduto, @qtdeProduto, @descontoProduto, @ipiProduto, @icmsProduto, @valorUnitProduto, @valorCustoProduto, @valorTotalProduto, @pesoProduto, @pesoLiqProduto);";

                                                using (SqlCommand cmd = new SqlCommand(insertOrUpdateQuery, connection))
                                                {
                                                    cmd.Parameters.AddWithValue("@idPedProduto", idPedProduto);
                                                    cmd.Parameters.AddWithValue("@idPedidoProduto", idPedidoProduto);
                                                    cmd.Parameters.AddWithValue("@idProduto", idProduto);
                                                    cmd.Parameters.AddWithValue("@descricaoProduto", descricaoProduto);
                                                    cmd.Parameters.AddWithValue("@qtdeProduto", qtdeProduto);
                                                    cmd.Parameters.AddWithValue("@descontoProduto", descontoProduto);
                                                    cmd.Parameters.AddWithValue("@ipiProduto", ipiProduto);
                                                    cmd.Parameters.AddWithValue("@icmsProduto", icmsProduto);
                                                    cmd.Parameters.AddWithValue("@valorUnitProduto", valorUnitProduto);
                                                    cmd.Parameters.AddWithValue("@valorCustoProduto", valorCustoProduto);
                                                    cmd.Parameters.AddWithValue("@valorTotalProduto", valorTotalProduto);
                                                    cmd.Parameters.AddWithValue("@pesoProduto", pesoProduto);
                                                    cmd.Parameters.AddWithValue("@pesoLiqProduto", pesoLiqProduto);
                                                    await cmd.ExecuteNonQueryAsync();

                                                Console.WriteLine($"Produto ID {idPedProduto} do Pedido ID {idPedidoProduto}: {descricaoProduto}");
                                                Thread.Sleep(10); // Aguarde  segundo (pode ajustar o valor conforme necessário) -> muitas requisições não estavam indo
                                                    Console.WriteLine($"Detalhes do Produto: {produto}");

                                                }

                                                }
                                            }
                                        }
                                        else
                                        {
                                            Console.WriteLine($"Falha na solicitação dos produtos para o pedido ID {idPedido}. Status Code: {produtosResponse.StatusCode}");
                                        }
                                    }
                                }

                                if (!hasTodayOrders)
                                {
                                    Console.WriteLine("Não há mais pedidos do dia atual. Encerrando a aplicação.");
                                    break; // Encerra o loop e finaliza a execução do programa
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
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro: {ex.Message}");
        }
    }

    // Função para converter JToken para DateTime ou retornar nulo
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

        static DateTime ObterDataUltimaAtualizacaoP2(SqlConnection connection, string campo)
    {
        using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = $"SELECT {campo} FROM CTRJOBS";
            object result = command.ExecuteScalar();
            if (result != DBNull.Value && result != null)
            {
                return (DateTime)result;
            }
            return DateTime.MinValue; // Retorna uma data mínima se não houver valor na tabela de controle
        }
    }

    static void AtualizarDataUltimaAtualizacaoP2(SqlConnection connection, string campo)
    {
        DateTime dataUltimaAtualizacaoP2 = DateTime.Now; // Defina a data e hora da atualização

        using (SqlCommand command = connection.CreateCommand())
        {
            // Atualize a coluna apropriada com a data e hora da atualização
            command.CommandText = $"UPDATE CTRJOBS SET {campo} = @dataUltimaAtualizacaoP2";
            command.Parameters.AddWithValue("@dataUltimaAtualizacaoP2", dataUltimaAtualizacaoP2);
            command.ExecuteNonQuery();
        }
    }
}
