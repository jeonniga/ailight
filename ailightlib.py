# for PySPARK
from pyspark.sql.session import SparkSession

# for Hive
import jaydebeapi

# for REST API
import urllib.request as ul
import xmltodict
import json
import sys
import io
import pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')


import numpy as np

from sklearn.preprocessing import LabelEncoder
from tensorflow.python.keras.models import Sequential
from tensorflow.python.keras.layers.embeddings import Embedding
from tensorflow.python.keras.layers.recurrent_v2 import LSTM
from tensorflow.python.keras.layers.core import Dense, Dropout
from tensorflow.python.keras.saving.save import load_model



class Data:
    def __init__(self, spark):
        self.spark = spark

    # csv file을 spark의 dataframe으로 리턴
    def opencsv(self, filename, tablename):        
        df_fiberfox = spark.read.csv(filename, inferSchema = True, header = True)
        df_fiberfox.createOrReplaceTempView(tablename)

        return df_fiberfox


    # dataframe의 특정 시리즈를 선택
    def modifydframe(self, sql):
        # 작업일자,시간,함체번호,작업담당자,온도,기압,습도,풍향,풍속,추정로스
        df = spark.sql(sql)

        return df


    # dataframe의 특정 시리즈 인코딩하여 pandas로 리턴
    def hotencoding(self, df, colname):
        pd = df.toPandas()
        encoder = LabelEncoder()
        encoder.fit(pd[colname])
        pd[colname] = encoder.transform(pd[colname])

        return pd

    # from maria to spark dataframe service
    def mariaToSpark(self, spark, constring, tablename, id, pwd):
        pgDF = spark.read \
        .format("jdbc") \
        .option("url", constring) \
        .option("dbtable", tablename) \
        .option("user", id) \
        .option("password", pwd) \
        .load()

        return pgDF.toPandas()


    # 하이브 정보를 읽어서 보여주는 서비스
    def getHiveInfo(self, durl, jar, sql):
        
        conn = jaydebeapi.connect(jclassname="com.cloudera.hive.jdbc4.HS2Driver",
                                url=durl, 
                                jars=jar,)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        
        return results


    # 하이브 실행(DML, DDL) 서비스
    def hiveExec(self, durl, jar, sql):
        
        conn = jaydebeapi.connect(jclassname="com.cloudera.hive.jdbc4.HS2Driver",
                                url=durl, 
                                jars=jar,)
        cursor = conn.cursor()
        cursor.execute(sql)
        
        return sql    


    # pandas의 자료구조를 dataframe으로 바꾸어 값을 선별하고 pandas로 다시 리턴
    def getPandasToDF(self, pd, schema, condition):
        df2 = spark.createDataFrame(pd, schema)
        df3 = df2.filter(condition)

        return df3.toPandas()


    def lstTodf(self, lstdf, columns, numbers):
        df = pd.DataFrame(lstdf, columns=columns)
        for x in numbers:
            df[x] = pd.to_numeric(df[x])

        return df


    # 데이터셋을 학습데이터와 결과데이터로 분리
    def getXY(self, df):
        # 온도, 기압, 습도 정보를 학습하기 위한 배열
        x_train = df.values[:,-6:-1]
        # 추정로스 값
        y_train = df.values[:,-1]

        # 텐서형으로 형식 변환
        X = np.asarray(x_train, dtype=np.float32)
        Y = np.asarray(y_train, dtype=np.float32)
        return X, Y       


    def getREST(self, url, code):
        pass





class LineData(Data):
    def __init__(self, spark):
        self.spark = spark

    def printSpecs(self):
        print(self.spark)

    def getREST(self, url, code):
        return super().getREST(url, code)        


class SparkData(Data):
    def __init__(self, spark):
        self.spark = spark

    def printSpecs(self):
        print(self.spark)

    def getREST(self, url, code):
        return super().getREST(url, code)        


class ClimateData(Data):
    def __init__(self, spark):
        self.spark = spark


    def printSpecs(self):
        print(self.spark)

    
    # 기상청 동네 예보 정보 보여주는 서비스
    def getREST(self, url, code):
        if code==1:
           df = self.getWdataHistory(url)
        elif code==2:
           df = self.getWdataTown(url)
        
        return df


    # 기상청 관측정보 서비스
    def getWdataHistory(self, url):
        #데이터를 받을 url
        request = ul.Request(url)
        #url의 데이터를 요청함
        response = ul.urlopen(request)
        #요청받은 데이터를 열어줌
        rescode = response.getcode()
        
        #제대로 데이터가 수신됐는지 확인하는 코드 성공시 200
        if(rescode == 200):
            responseData = response.read()
            #요청받은 데이터를 읽음
            rD = xmltodict.parse(responseData)
            #XML형식의 데이터를 dict형식으로 변환시켜줌
            rDJ = json.dumps(rD)
            #dict 형식의 데이터를 json형식으로 변환
            rDD = json.loads(rDJ)
            
            #json형식의 데이터를 dict 형식으로 변환
            dic = rDD['response']['body']['items']
            df = pd.DataFrame(dic['item'])
        else:
            print(rescode)

        return df  
    

    # 기상청 동네 예보 정보 보여주는 서비스
    def getWdataTown(self, url):
        request = ul.Request(url)
        #url의 데이터를 요청함

        response = ul.urlopen(request)
        #요청받은 데이터를 열어줌

        rescode = response.getcode()
        #제대로 데이터가 수신됐는지 확인하는 코드 성공시 200
        if(rescode == 200):
            responseData = response.read()
            #요청받은 데이터를 읽음
            rD = xmltodict.parse(responseData)
            #XML형식의 데이터를 dict형식으로 변환시켜줌

            rDJ = json.dumps(rD)
            #dict 형식의 데이터를 json형식으로 변환
            rDD = json.loads(rDJ)
            
            #json형식의 데이터를 dict 형식으로 변환
            dic = rDD['response']['body']['items']
            df = pd.DataFrame(dic['item'])


        return df


class Model():
    

    # 모델 생성
    def getModel(self, max_features):
        # 연쇄 방식 모델링 포함 함수형 구현
        model = Sequential()
        # 임베딩 테이블 레이어 생성 (1024 * 3 = 3072의 param)
        model.add(Embedding(max_features, output_dim=5))
        # 메모리셀 128개
        model.add(LSTM(128))
        # 오버피팅을 막기위해 dropout율 0.5% (학습할때 연결된 노드 연결을 무시, 그만둠)
        model.add(Dropout(0.5))
        # 은닉층 추가, 활성함수 sigmoid.
        model.add(Dense(1, activation='sigmoid'))

        # 모델 컴파일 
        model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
        return model


    # 모델을 json으로 저장
    def savemodel(self, model, fname):
        # 학습된 모델 저장
        model_json = model.to_json()
        with open(fname, "w") as json_file : 
            json_file.write(model_json)


    # json을 모델로 불러옴
    def loadmodel(self, fname):
        # 저장된 모델 불러오기
        from keras.models import model_from_json 
        json_file = open(fname, "r") 
        loaded_model_json = json_file.read() 
        json_file.close() 
        loaded_model = model_from_json(loaded_model_json)

        return loaded_model

    # 학습경과 저장 서비스
    def SaveStudyResult(self, history):
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        print(history.history)
        loss = history.history['loss']
        accuracy = history.history['accuracy']
        epochs = range(0,10)
        plt.plot(epochs, loss, 'g', label='Training loss')
        plt.plot(epochs, accuracy, 'b', label='validation loss')
        plt.title('Training and Validation loss')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        # plt.show()

        plt.savefig('./static/img/TempvsPressure.png')
        return '/static/img/studycurv.png'      




if __name__=='__main__':
    spark = SparkSession.builder.appName("fiberfox")\
    .config("spark.driver.extraClassPath",
         "C:/spark-3.0/jars/mariadb-java-client-1.1.10.jar")\
    .getOrCreate() 

    #line_data = LineData(spark)
    # line_data.printSpecs()

    #df = line_data.opencsv("fiberfox.csv","fiberfox_raw")
    # print(df)
    
    #df = line_data.mariaToSpark("jdbc:mysql://127.0.0.1:3306/test","fiberfox_raw", "root", "9907")
    # print(df)

    """
    lstdf = line_data.getHiveInfo('jdbc:hive2://121.183.206.41:10000/test;',
            'HiveJDBC4.jar',
            'select workdate, temperature, pressure, um from default.fiberfox')

    df = line_data.lstTodf(lstdf, columns=['workdate', 'temperature', 'pressure', 'um'], numbers=['temperature', 'pressure', 'um'])
    ddf = df.pivot_table(index='temperature',columns=['temperature', 'pressure'], values='um')
  
    import matplotlib.pyplot as plt
    import seaborn as sns 
    sns.heatmap(ddf, cmap='YlGnBu') # 
    plt.title('temp. vs pressure', fontsize=20)
    plt.show()
    #plt.savefig('test.png')
    """

    cdata = ClimateData(spark)
    #df = cdata.getREST("http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList?serviceKey=iKk6l9e%2F8SmdYVCFKoRWVlgQHQ2F8hNtTAOiwOhu6LrXO507c3HqEr4P%2BaQwJVgESUzIHU5j63ajbyScF9pgXw%3D%3D&pageNo=1&numOfRows=10&dataType=XML&dataCd=ASOS&dateCd=DAY&startDt=20200101&endDt=20200723&stnIds=133&", 1)
    #print(df)

    df = cdata.getREST("http://apis.data.go.kr/1360000/VilageFcstInfoService/getVilageFcst?serviceKey=iKk6l9e%2F8SmdYVCFKoRWVlgQHQ2F8hNtTAOiwOhu6LrXO507c3HqEr4P%2BaQwJVgESUzIHU5j63ajbyScF9pgXw%3D%3D&pageNo=1&numOfRows=10&dataType=XML&base_date=20200726&base_time=0500&nx=1&ny=1&",2)
    #print(df)
    
    ddf = cdata.lstTodf(df, columns=['fcstDate', 'fcstTime', 'nx', 'ny', 'category', 'fcstValue'], numbers=['fcstValue'])
    
    #ddf = df.pivot_table(index=['fcstDate','fcstTime'], columns=['category'], values=['fcstValue'])
    #print(ddf)


    skyflag = 1 if ddf.query('category=="SKY"')['fcstValue'].count()==0 else ddf.query('category=="SKY"')['fcstValue'].values[0]
    

    if skyflag==1:
        sky='맑음'
    elif skyflag==3:
        sky='구름많음'
    elif skyflag==4:
        sky='흐림'
    

    rainratio = 0 if ddf.query('category=="POP"')['fcstValue'].count()==0 else ddf.query('category=="POP"')['fcstValue'].values[0]
    amountofrain = 0 if ddf.query('category=="R06"')['fcstValue'].count()==0 else ddf.query('category=="R06"')['fcstValue'].values[0]
    hightemp = 0 if ddf.query('category=="TMX"')['fcstValue'].count()==0 else ddf.query('category=="TMX"')['fcstValue'].values[0]
    lowtemp = 0 if ddf.query('category=="TMN"')['fcstValue'].count()==0 else ddf.query('category=="TMN"')['fcstValue'].values[0]
    curtemp = 0 if ddf.query('category=="T3H"')['fcstValue'].count()==0 else ddf.query('category=="T3H"')['fcstValue'].values[0]
    humidity = 0 if ddf.query('category=="REH"')['fcstValue'].count()==0 else ddf.query('category=="REH"')['fcstValue'].values[0]
    
    
    print('날씨 : ', sky)
    print('강수확률 : ', rainratio)
    print('강수량 : ', amountofrain)
    print('최고온도 : ', hightemp)
    print('최저온도 : ', lowtemp)
    print('온도 : ', curtemp)
    print('습도 : ', humidity)
    
    ddf = ddf.drop_duplicates(['fcstDate','fcstTime','nx','ny'], keep='first')
    ddfinal = ddf.drop(columns=['category','fcstValue'])
    # df4.drop_duplicates(['id', 'desc'], keep='last')
    

    ddfinal['sky'] = sky
    ddfinal['rainratio'] = rainratio
    ddfinal['amountofrain'] = amountofrain
    ddfinal['hightemp'] = hightemp
    ddfinal['lowtemp'] = lowtemp
    ddfinal['curtemp'] = curtemp
    ddfinal['humidity'] = humidity
    print(ddfinal)