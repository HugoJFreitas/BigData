from pyspark.sql import SparkSession, HiveContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType


def create_session():

    # sc = SparkSession \
    #     .builder \
    #     .appName("Joining-Data") \
    #     .enableHiveSupport() \
    #     .getOrCreate()
    sc = (SparkSession \
        .builder \
        .appName('Joining-Data') \
        .enableHiveSupport() \
        .getOrCreate())

    return sc

def extract_vacs(sc, vacs_file):

    schema = StructType([ 
        StructField("Data", DateType()),
        StructField("UF", StringType()),
        StructField("Suspeitos", IntegerType()),
        StructField("Testes", IntegerType()),
        StructField("Testes_100k", FloatType()),
        StructField("PrimeiraDose", IntegerType()),
        StructField("PrimeiraDose_100k", FloatType()),
        StructField("SegundaDose", IntegerType()),
        StructField("SegundaDose_100k", FloatType()),
        StructField("DoseUnica", IntegerType()),
        StructField("DoseUnica_100k", FloatType()),
        StructField("TerceiraDose", IntegerType()),
        StructField("TerceiraDose_100k", FloatType())
        ])
    
    df = sc.read.csv(vacs_file, schema=schema)

    drop_columns = ["Suspeitos", "Testes", "Testes_100k"]

    df = df.drop(*drop_columns)

    return df



def extract_cases(sc, cases_file):
    
    schema  = StructType([
        StructField("UID", IntegerType()),
        StructField("UF", StringType()),
        StructField("Estado", StringType()),
        StructField("Casos", IntegerType()),
        StructField("Obitos", IntegerType()),
        StructField("Suspeitos", IntegerType()),
        StructField("Descartados", IntegerType()),
        StructField("Transmissao", IntegerType()),
        StructField("Comentarios", StringType()),
        StructField("Data", DateType()),
        StructField("Data_Insercao", DateType())
    ])

    df = sc.read.csv(cases_file, schema=schema)

    drop_columns = ["UID", "Descartados", "Transmissao", "Comentarios", "Data_Insercao"]
   
    df = df.drop(*drop_columns)
       
    return df

def join_dfs(sc, vacs, cases):

    vacs.createOrReplaceTempView("VACS")
    cases.createOrReplaceTempView("CASES")

    fact_df = sc.sql("select v.Data, v.UF, c.Estado, c.Suspeitos, c.Casos, c.Obitos, v.DoseUnica, v.PrimeiraDose, v.SegundaDose, v.TerceiraDose from VACS v, CASES c \
            where v.Data = c.Data AND v.UF == c.UF")
    
    fact_df.createOrReplaceTempView("FACT")

    fact_df = sc.sql("select * from FACT f where f.UF == 'MG'")
    # fact_df = fact_df.na.drop(thresh=7)


    return fact_df

def load_fact(sc, fact_df):
    

    fact_df.write.mode("overwrite").saveAsTable("dw_covid.f_covid")



if __name__ == '__main__':

    vacs_file = "hdfs:///user/hadoopuser/datalake/covid19/WCota_Vacinas_Estados/part-m-00000"
    cases_file = "hdfs:///user/hadoopuser/datalake/covid19/Brasil_api_base_nacional/part-m-00000"

    sc = create_session()

    vacs = extract_vacs(sc, vacs_file)
    cases = extract_cases(sc, cases_file)

    fact_df = join_dfs(sc, vacs, cases)

    # sc.sql('show databases').show()
    # fact_df.show(30000)
    load_fact(sc, fact_df)

    fact_df.show(n=20)
