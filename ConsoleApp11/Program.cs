using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace ConsoleApp11
{
    public static class ProductCategoryAnalyzer
    {
        public static DataFrame GetProductCategories(DataFrame products, DataFrame categories)
        {
            // Переименование колонок для объединения
            products = products.WithColumnRenamed("product_id", "product");
            categories = categories.WithColumnRenamed("category_id", "category");

            // Объединение данных по ключу "product_id" (left join)
            var joinedData = products.Join(categories, products["product"] == categories["product"], "left")
                .Select(products["product"], Coalesce(categories["category"], Lit("No Category")).Alias("category"));

            return joinedData;
        }
    }

    internal class Program
    {
        static void Main(string[] args)
        {
            // Создайте SparkSession
            SparkSession spark = SparkSession.Builder()
                .AppName("ProductCategories")
                .GetOrCreate();

            // Создайте DataFrame с данными о продуктах и категориях
            var data = new List<Row>
            {
                new GenericRow(new object[] { "Product1", "Category1" }),
                new GenericRow(new object[] { "Product2", "Category2" }),
                new GenericRow(new object[] { "Product3", "Category1" }),
                new GenericRow(new object[] { "Product4", null })
            };

            var schema = new StructType(new[]
            {
                new StructField("Product", new StringType()),
                new StructField("Category", new StringType())
            });

            DataFrame df = spark.CreateDataFrame((IEnumerable<GenericRow>)data, schema);

            df.CreateOrReplaceTempView("product_categories");

 
            DataFrame resultDf = spark.Sql("SELECT Product, COALESCE(Category, 'No Category') AS Category FROM product_categories");

            resultDf.Show();

            spark.Stop();
        }
    }
}
