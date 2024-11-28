using System;
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
                await connection.OpenAsync();

                if (connection.State != ConnectionState.Open)
                {
                    throw new Exception("Erro na conexão com o banco de dados.");
                }

                string accessToken = "token";
                string secretAccessToken = "secret";
                int pageSize = 250;
                int offset = 0;

                HttpClient httpClient = new HttpClient();
                httpClient.DefaultRequestHeaders.Add("access-token", accessToken);
                httpClient.DefaultRequestHeaders.Add("secret-access-token", secretAccessToken);
                httpClient.Timeout = TimeSpan.FromSeconds(60); // Definindo o tempo limite para 60 segundos

                while (true)
                {
                    HttpResponseMessage response = await httpClient.GetAsync($"https://api.vhsys.com/v2/notas-fiscais?offset={offset}&limit={pageSize}&order=data_mod_pedido&sort=Desc").ConfigureAwait(false);

                    if (response.IsSuccessStatusCode)
                    {
                        string jsonResponse = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        JObject data = JObject.Parse(jsonResponse);

                        if (data["data"] is JArray notasfiscaisArray)
                        {
                            int numberOfnotasfiscais = notasfiscaisArray.Count;
                            Console.WriteLine($"Número de notas fiscais na página offset {offset}: {numberOfnotasfiscais}");

                            bool hasTodayOrders = false;

                            foreach (var notafiscal in notasfiscaisArray)
                            {
                                DateTime? dataModPedido = GetDateTimeOrNull(notafiscal, "data_mod_pedido");

                                if (dataModPedido.HasValue && dataModPedido.Value.Date == DateTime.Today)
                                {
                                    hasTodayOrders = true;

                                    int idNotaFiscal = (int)notafiscal["id_venda"];

                                    string url = $"https://api.vhsys.com/v2/notas-fiscais/{idNotaFiscal}/produtos";
                                    HttpResponseMessage notasfiscaisResponse = await httpClient.GetAsync(url).ConfigureAwait(false);

                                    if (notasfiscaisResponse.IsSuccessStatusCode)
                                    {
                                        string notasficaisJsonResponse = await notasfiscaisResponse.Content.ReadAsStringAsync().ConfigureAwait(false);
                                        JObject notasfiscais = JObject.Parse(notasficaisJsonResponse);

                                        if (notasfiscais["data"] is JArray produtosArray)
                                        {
                                            foreach (var produto in produtosArray)
                                            {
                                                int idPedProduto = (int)produto["id_ped_produto"];
                                                int idVenda = (int)produto["id_venda"];
                                                int idProduto = (int)produto["id_produto"];
                                                string codProduto = (string)produto["cod_produto"];
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
                                                string vBCProduto = (string)produto["vBC_produto"];
                                                string vBCProdutoEditado = (string)produto["vBC_produto_editado"];
                                                int vBCProdutoIsEditado = (int)produto["vBC_produto_is_editado"];
                                                string vICMSProduto = (string)produto["vICMS_produto"];
                                                string vBCIpiProduto = (string)produto["vBCipi_produto"];
                                                string vIPIProduto = (string)produto["vIPI_produto"];
                                                string vBCIpiEditado = (string)produto["vBC_ipi_editado"];
                                                string vBCPisEditado = (string)produto["vBC_pis_editado"];
                                                int vBCPisIsEditado = (int)produto["vBC_pis_is_editado"];
                                                string vBCCofinsEditado = (string)produto["vBC_cofins_editado"];
                                                int vBCCofinsIsEditado = (int)produto["vBC_cofins_is_editado"];
                                                int vBCIpiIsEditado = (int)produto["vBC_ipi_is_editado"];
                                                string vBCSTProduto = (string)produto["vBCST_produto"];
                                                string vSTProduto = (string)produto["vST_produto"];
                                                int cfopProduto = (int)produto["cfop_produto"];
                                                string ncmProduto = (string)produto["ncm_produto"];
                                                string unidadeProduto = (string)produto["unidade_produto"];
                                                string unidadeTributavel = (string)produto["unidade_tributavel"];
                                                decimal qtdeTributavel = (decimal)produto["qtde_tributavel"];
                                                decimal valorUnitTributavel = (decimal)produto["valor_unit_tributavel"];
                                                string beneficioFiscal = (string)produto["beneficio_fiscal"];
                                                int origemProduto = (int)produto["origem_produto"];
                                                string infoAdicional = (string)produto["info_adicional"];
                                                string ordemProduto = (string)produto["ordem_produto"];
                                                string codigoBarras = (string)produto["codigo_barras"];
                                                string codigoBarrasInternos = (string)produto["codigo_barras_internos"];
                                                decimal valorTributosUnit = (decimal)produto["valor_tributos_unit"];
                                                decimal valorTributosTotal = (decimal)produto["valor_tributos_total"];
                                                int valorTributosTotalIsEditado = (int)produto["valor_tributos_total_is_editado"];
                                                decimal valorTributosEstUnit = (decimal)produto["valor_tributosEst_unit"];
                                                decimal valorTributosEstTotal = (decimal)produto["valor_tributosEst_total"];
                                                int valorTributosEstTotalIsEditado = (int)produto["valor_tributosEst_total_is_editado"];
                                                decimal valorDesconto = (decimal)produto["valor_desconto"];
                                                decimal valorFrete = (decimal)produto["valor_frete"];
                                                decimal valorOutros = (decimal)produto["valor_outros"];
                                                string fciProduto = (string)produto["fci_produto"];
                                                decimal valorSeguro = (decimal)produto["valor_seguro"];
                                                int idAlmoxarifado = (int)produto["id_almoxarifado"];
                                                int idLoteAlmoxarifado = (int)produto["id_lote_almoxarifado"];
                                                string numeroLoteAlmoxarifado = (string)produto["numero_lote_almoxarifado"];
                                                DateTime? dataFabricacaoAlmoxarifado = (DateTime?)produto["data_fabricacao_almoxarifado"];
                                                DateTime? dataValidadeAlmoxarifado = (DateTime?)produto["data_validade_almoxarifado"];


                                                string insertOrUpdateQuery = @"
                                                MERGE INTO NF2 AS target
                                                USING (SELECT @idVenda AS id_venda) AS source
                                                ON (target.id_venda = source.id_venda)
                                                WHEN MATCHED THEN
                                                    UPDATE SET
                                                        target.id_ped_produto = @idPedProduto,  
                                                        target.id_produto = @idProduto,
                                                        target.cod_produto = @codProduto,  
                                                        target.descricao = @descProduto,  
                                                        target.qtde_produto = @qtdeProduto,
                                                        target.desconto_produto = @descontoProduto,
                                                        target.ipi_produto = @ipiProduto,
                                                        target.icms_produto = @icmsProduto,
                                                        target.valor_unit_produto = @valorUnitProduto,
                                                        target.valor_custo_produto = @valorCustoProduto,
                                                        target.valor_total_produto = @valorTotalProduto,
                                                        target.vBC_produto = @vBCProduto,
                                                        target.vBC_produto_editado = @vBCProdutoEditado,
                                                        target.vBC_produto_is_editado = @vBCProdutoIsEditado,
                                                        target.vICMS_produto = @vICMSProduto,
                                                        target.vBCipi_produto = @vBCIpiProduto,
                                                        target.vIPI_produto = @vIPIProduto,
                                                        target.vBC_ipi_editado = @vBCIpiEditado,
                                                        target.vBC_pis_editado = @vBCPisEditado,
                                                        target.vBC_pis_is_editado = @vBCPisIsEditado,
                                                        target.vBC_cofins_editado = @vBCCofinsEditado,
                                                        target.vBC_cofins_is_editado = @vBCCofinsIsEditado,
                                                        target.vBC_ipi_is_editado = @vBCIpiIsEditado,
                                                        target.vBCST_produto = @vBCSTProduto,
                                                        target.vST_produto = @vSTProduto,
                                                        target.cfop_produto = @cfopProduto,
                                                        target.ncm_produto = @ncmProduto,
                                                        target.unidade_produto = @unidadeProduto,
                                                        target.unidade_tributavel = @unidadeTributavel,
                                                        target.qtde_tributavel = @qtdeTributavel,
                                                        target.valor_unit_tributavel = @valorUnitTributavel,
                                                        target.beneficio_fiscal = @beneficioFiscal,
                                                        target.origem_produto = @origemProduto,
                                                        target.peso_produto = @pesoProduto,
                                                        target.peso_liq_produto = @pesoLiqProduto,
                                                        target.info_adicional = @infoAdicional,
                                                        target.ordem_produto = @ordemProduto,
                                                        target.codigo_barras = @codigoBarras,
                                                        target.codigo_barras_internos = @codigoBarrasInternos,
                                                        target.valor_tributos_unit = @valorTributosUnit,
                                                        target.valor_tributos_total = @valorTributosTotal,
                                                        target.valor_tributos_total_is_editado = @valorTributosTotalIsEditado,
                                                        target.valor_tributosEst_unit = @valorTributosEstUnit,
                                                        target.valor_tributosEst_total = @valorTributosEstTotal,
                                                        target.valor_tributosEst_total_is_editado = @valorTributosEstTotalIsEditado,
                                                        target.valor_desconto = @valorDesconto,
                                                        target.valor_frete = @valorFrete,
                                                        target.valor_outros = @valorOutros,
                                                        target.fci_produto = @fciProduto,
                                                        target.valor_seguro = @valorSeguro,
                                                        target.id_almoxarifado = @idAlmoxarifado,
                                                        target.id_lote_almoxarifado = @idLoteAlmoxarifado,
                                                        target.numero_lote_almoxarifado = @numeroLoteAlmoxarifado,
                                                        target.data_fabricacao_almoxarifado = @dataFabricacaoAlmoxarifado,
                                                        target.data_validade_almoxarifado = @dataValidadeAlmoxarifado
                                                WHEN NOT MATCHED THEN
                                                    INSERT (id_venda, id_ped_produto, id_produto, cod_produto, descricao, qtde_produto, desconto_produto, ipi_produto, icms_produto, valor_unit_produto, valor_custo_produto, valor_total_produto, vBC_produto, vBC_produto_editado, vBC_produto_is_editado, vICMS_produto, vBCipi_produto, vIPI_produto, vBC_ipi_editado, vBC_pis_editado, vBC_pis_is_editado, vBC_cofins_editado, vBC_cofins_is_editado, vBC_ipi_is_editado, vBCST_produto, vST_produto, cfop_produto, ncm_produto, unidade_produto, unidade_tributavel, qtde_tributavel, valor_unit_tributavel, beneficio_fiscal, origem_produto, peso_produto, peso_liq_produto, info_adicional, ordem_produto, codigo_barras, codigo_barras_internos, valor_tributos_unit, valor_tributos_total, valor_tributos_total_is_editado, valor_tributosEst_unit, valor_tributosEst_total, valor_tributosEst_total_is_editado, valor_desconto, valor_frete, valor_outros, fci_produto, valor_seguro, id_almoxarifado, id_lote_almoxarifado, numero_lote_almoxarifado, data_fabricacao_almoxarifado, data_validade_almoxarifado)
                                                    VALUES (@idVenda, @idPedProduto, @idProduto, @codProduto, @descProduto, @qtdeProduto, @descontoProduto, @ipiProduto, @icmsProduto, @valorUnitProduto, @valorCustoProduto, @valorTotalProduto, @vBCProduto, @vBCProdutoEditado, @vBCProdutoIsEditado, @vICMSProduto, @vBCIpiProduto, @vIPIProduto, @vBCIpiEditado, @vBCPisEditado, @vBCPisIsEditado, @vBCCofinsEditado, @vBCCofinsIsEditado, @vBCIpiIsEditado, @vBCSTProduto, @vSTProduto, @cfopProduto, @ncmProduto, @unidadeProduto, @unidadeTributavel, @qtdeTributavel, @valorUnitTributavel, @beneficioFiscal, @origemProduto, @pesoProduto, @pesoLiqProduto, @infoAdicional, @ordemProduto, @codigoBarras, @codigoBarrasInternos, @valorTributosUnit, @valorTributosTotal, @valorTributosTotalIsEditado, @valorTributosEstUnit, @valorTributosEstTotal, @valorTributosEstTotalIsEditado, @valorDesconto, @valorFrete, @valorOutros, @fciProduto, @valorSeguro, @idAlmoxarifado, @idLoteAlmoxarifado, @numeroLoteAlmoxarifado, @dataFabricacaoAlmoxarifado, @dataValidadeAlmoxarifado);
                                                ";

                                                using (SqlCommand cmd = new SqlCommand(insertOrUpdateQuery, connection))
                                                {
                                                    cmd.Parameters.AddWithValue("@idPedProduto", (int)produto["id_ped_produto"]);
                                                    cmd.Parameters.AddWithValue("@idVenda", (int)produto["id_venda"]);
                                                    cmd.Parameters.AddWithValue("@idProduto", (int)produto["id_produto"]);
                                                    cmd.Parameters.AddWithValue("@codProduto", (string)produto["cod_produto"]);
                                                    cmd.Parameters.AddWithValue("@descProduto", (string)produto["desc_produto"]);
                                                    cmd.Parameters.AddWithValue("@qtdeProduto", (decimal)produto["qtde_produto"]);
                                                    cmd.Parameters.AddWithValue("@descontoProduto", (decimal)produto["desconto_produto"]);
                                                    cmd.Parameters.AddWithValue("@ipiProduto", (decimal)produto["ipi_produto"]);
                                                    cmd.Parameters.AddWithValue("@icmsProduto", (decimal)produto["icms_produto"]);
                                                    cmd.Parameters.AddWithValue("@valorUnitProduto", (decimal)produto["valor_unit_produto"]);
                                                    cmd.Parameters.AddWithValue("@valorCustoProduto", (decimal)produto["valor_custo_produto"]);
                                                    cmd.Parameters.AddWithValue("@valorTotalProduto", (decimal)produto["valor_total_produto"]);
                                                    cmd.Parameters.AddWithValue("@vBCProduto", (decimal)produto["vBC_produto"]);
                                                    cmd.Parameters.AddWithValue("@vBCProdutoEditado", (string)produto["vBC_produto_editado"]);
                                                    cmd.Parameters.AddWithValue("@vBCProdutoIsEditado", (int)produto["vBC_produto_is_editado"]);
                                                    cmd.Parameters.AddWithValue("@vICMSProduto", (decimal)produto["vICMS_produto"]);
                                                    cmd.Parameters.AddWithValue("@vBCIpiProduto", (decimal)produto["vBCipi_produto"]);
                                                    cmd.Parameters.AddWithValue("@vIPIProduto", (decimal)produto["vIPI_produto"]);
                                                    cmd.Parameters.AddWithValue("@vBCIpiEditado", (string)produto["vBC_ipi_editado"]);
                                                    cmd.Parameters.AddWithValue("@vBCPisEditado", (string)produto["vBC_pis_editado"]);
                                                    cmd.Parameters.AddWithValue("@vBCPisIsEditado", (int)produto["vBC_pis_is_editado"]);
                                                    cmd.Parameters.AddWithValue("@vBCCofinsEditado", (string)produto["vBC_cofins_editado"]);
                                                    cmd.Parameters.AddWithValue("@vBCCofinsIsEditado", (int)produto["vBC_cofins_is_editado"]);                                                        
                                                    cmd.Parameters.AddWithValue("@vBCIpiIsEditado", (int)produto["vBC_ipi_is_editado"]);
                                                    cmd.Parameters.AddWithValue("@vBCSTProduto", (decimal)produto["vBCST_produto"]);
                                                    cmd.Parameters.AddWithValue("@vSTProduto", (decimal)produto["vST_produto"]);
                                                    cmd.Parameters.AddWithValue("@cfopProduto", (int)produto["cfop_produto"]);
                                                    cmd.Parameters.AddWithValue("@ncmProduto", (string)produto["ncm_produto"]);
                                                    cmd.Parameters.AddWithValue("@unidadeProduto", (string)produto["unidade_produto"]);
                                                    cmd.Parameters.AddWithValue("@unidadeTributavel", (string)produto["unidade_tributavel"]);
                                                    cmd.Parameters.AddWithValue("@qtdeTributavel", (decimal)produto["qtde_tributavel"]);                                                        
                                                    cmd.Parameters.AddWithValue("@valorUnitTributavel", (decimal)produto["valor_unit_tributavel"]);
                                                    cmd.Parameters.AddWithValue("@beneficioFiscal", (string)produto["beneficio_fiscal"]);
                                                    cmd.Parameters.AddWithValue("@origemProduto", (int)produto["origem_produto"]);
                                                    cmd.Parameters.AddWithValue("@pesoProduto", (decimal)produto["peso_produto"]);
                                                    cmd.Parameters.AddWithValue("@pesoLiqProduto", (decimal)produto["peso_liq_produto"]);
                                                    cmd.Parameters.AddWithValue("@infoAdicional", (string)produto["info_adicional"]);
                                                    cmd.Parameters.AddWithValue("@ordemProduto", (string)produto["ordem_produto"]);
                                                    cmd.Parameters.AddWithValue("@codigoBarras", (string)produto["codigo_barras"]);
                                                    cmd.Parameters.AddWithValue("@codigoBarrasInternos", DBNull.Value); // Assuming this field is not present in the JSON
                                                    cmd.Parameters.AddWithValue("@valorTributosUnit", (decimal)produto["valor_tributos_unit"]);
                                                    cmd.Parameters.AddWithValue("@valorTributosTotal", (decimal)produto["valor_tributos_total"]);
                                                    cmd.Parameters.AddWithValue("@valorTributosTotalIsEditado", (int)produto["valor_tributos_total_is_editado"]);
                                                    cmd.Parameters.AddWithValue("@valorTributosEstUnit", (decimal)produto["valor_tributosEst_unit"]);
                                                    cmd.Parameters.AddWithValue("@valorTributosEstTotal", (decimal)produto["valor_tributosEst_total"]);                                                        
                                                    cmd.Parameters.AddWithValue("@valorTributosEstTotalIsEditado", (int)produto["valor_tributosEst_total_is_editado"]);
                                                    cmd.Parameters.AddWithValue("@valorDesconto", (decimal)produto["valor_desconto"]);
                                                    cmd.Parameters.AddWithValue("@valorFrete", (decimal)produto["valor_frete"]);
                                                    cmd.Parameters.AddWithValue("@valorOutros", (decimal)produto["valor_outros"]);
                                                    cmd.Parameters.AddWithValue("@fciProduto", DBNull.Value); // Assuming this field is not present in the JSON
                                                    cmd.Parameters.AddWithValue("@valorSeguro", (decimal)produto["valor_seguro"]);
                                                    cmd.Parameters.AddWithValue("@idAlmoxarifado", (int)produto["id_almoxarifado"]);
                                                    cmd.Parameters.AddWithValue("@idLoteAlmoxarifado", (int)produto["id_lote_almoxarifado"]);
                                                    cmd.Parameters.AddWithValue("@numeroLoteAlmoxarifado", (string)produto["numero_lote_almoxarifado"]);
                                                    cmd.Parameters.AddWithValue("@dataFabricacaoAlmoxarifado", DBNull.Value); // Assuming this field is not present in the JSON
                                                    cmd.Parameters.AddWithValue("@dataValidadeAlmoxarifado", DBNull.Value); // Assuming this field is not present in the JSON

                                                    await cmd.ExecuteNonQueryAsync();
                                                    Console.WriteLine($"Dados inseridos ou atualizados para o idNotaFiscal {idNotaFiscal}.");
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if (!hasTodayOrders)
                            {
                                Console.WriteLine("Nenhuma nota fiscal encontrada para hoje.");
                                Console.WriteLine("Notas fiscais de hoje inseridos. Parando a execução do código.");
                                Environment.Exit(0); // Isso vai encerrar o programa
                                break;
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Falha ao obter notas fiscais. Código de status: {response.StatusCode}");
                        break;
                    }

                    offset += pageSize;

                    // Pause for a moment to prevent overwhelming the API
                    await Task.Delay(1000); // Delay for 1 second
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ocorreu um erro: {ex.Message}");
        }
    }

    static DateTime? GetDateTimeOrNull(JToken token, string propertyName)
    {
        if (token[propertyName] == null)
        {
            return null;
        }
        return token.Value<DateTime>(propertyName);
    }

    static string IdentificarCampoProblematico(JToken produto)
    {
        // Verifique cada campo no produto e retorne o nome do campo que pode estar causando o erro
        foreach (var property in produto.Children())
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

    static DateTime ObterDataUltimaAtualizacaoNF2(SqlConnection connection, string campo)
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

    static void AtualizarDataUltimaAtualizacaoNF2(SqlConnection connection, string campo)
    {
        DateTime dataUltimaAtualizacaoNF2 = DateTime.Now; // Defina a data e hora da atualização

        using (SqlCommand command = connection.CreateCommand())
        {
            // Atualize a coluna apropriada com a data e hora da atualização
            command.CommandText = $"UPDATE CTRJOBS SET {campo} = @dataUltimaAtualizacaoNF2";
            command.Parameters.AddWithValue("@dataUltimaAtualizacaoNF2", dataUltimaAtualizacaoNF2);
            command.ExecuteNonQuery();
        }
    }
}
