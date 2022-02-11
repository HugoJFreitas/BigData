from pyspark.sql import SparkSession, HiveContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import functions as F

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression



def create_session():

    sc = (SparkSession \
        .builder \
        .appName('Covid-LinReg') \
        .enableHiveSupport() \
        .getOrCreate())

    return sc

def get_data(sc):

    sc.sql("use dw_covid")
    df = sc.sql("select * from f_covid")

    return df

def lin_reg(sc, df):

    # non_features = ["Estado", "Obitos", "DoseUnica", "PrimeiraDose", "SegundaDose", "TerceiraDose", "Data"]
    non_features = ["UF", "Estado", "Obitos", "DoseUnica", "PrimeiraDose", "SegundaDose", "TerceiraDose", "Data"]

    df = df.withColumn("S_Data", df["Data"].cast(StringType()))

    df.show()

    features = df.drop(*non_features)
    features.show()
    
    # to_index_cols = ["S_Data", "UF"]
    to_index_cols = ["S_Data"]
    # indexer = StringIndexer(inputCols=to_index_cols, outputCols=["I_Data", "I_UF"])
    indexer = StringIndexer(inputCols=to_index_cols, outputCols=["I_Data"])

    indexed_model = indexer.fit(features)

    features = indexed_model.transform(features).drop(*to_index_cols)
    features.show(100)

    # assemble_cols = ["I_Data", "I_UF", "Suspeitos"]
    assemble_cols = ["I_Data", "Suspeitos"]
    assembler = VectorAssembler(inputCols=assemble_cols, outputCol="features")

    
    output = assembler.transform(features).drop(*assemble_cols)

    output.show()

    train, test = output.randomSplit([0.7, 0.3])

    lr = LinearRegression(featuresCol="features", labelCol='Casos')

    linear_model = lr.fit(train)
    print("Coefficients: " + str(linear_model.coefficients))
    print("\nIntercept: " + str(linear_model.intercept))

    trainSummary = linear_model.summary
    print("RMSE: %f" % trainSummary.rootMeanSquaredError)
    print("\nr2: %f" % trainSummary.r2)

    predictions = linear_model.transform(test)
    x =((predictions['Casos']-predictions['prediction'])/predictions['Casos'])*100
    predictions = predictions.withColumn('Accuracy', F.abs(x))

    predictions.show(n=20000)



if __name__ == '__main__':

    db_name = "dw_covid"
    cases_file = "hdfs:///user/hadoopuser/datalake/covid19/Brasil_api_base_nacional/part-m-00000"

    sc = create_session() #create session
    df = get_data(sc) #get data from hive

    df.printSchema()

    lin_reg(sc, df)