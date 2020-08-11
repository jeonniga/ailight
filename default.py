# pip install flask pyspark cmake
# pip install JPype1-0.7.1-cp35-cp35m-win_amd64.whl
# pip install JayDeBeApi xmltodict pandas sklearn
# pip install Flask-Babel matplotlib seaborn

# pybabel compile -d translations
from collections import namedtuple 
from flask import Flask, render_template, request, session
from ailightlib import Data, ClimateData, LineData, SparkData, Model

app = Flask(__name__)
wsgi_app = app.wsgi_app

#다국어
from flask_babel import Babel, gettext

babel = Babel(app)

@babel.localeselector
def get_locale():
    if request.args.get('lang'):
        session['lang'] = request.args.get('lang')
    return session.get('lang', 'en')


# 세션키 설정
app.secret_key = b'ytglobal@3400'


# for PySPARK
from pyspark.sql.session import SparkSession

# for Hive
import jaydebeapi

# make Heatmap
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns 

import datetime


Person = namedtuple('Person', ['firstname', 'lastname'])

class Controller:
    def main(self):
        person = Service().addUser('학수', '임')
        # user_name_label=gettext('user_name_label'))
        return render_template('./start/main.html', model=person, locale=get_locale())

    def weathermap(self):
        person = Service().addUser('HAKSOO', 'LIM')
        return render_template('./dashboard/기상도.html', model=person)

    def weatherpredict(self):
        weather = Service().weatherpredict()
        return render_template('./dashboard/기상청예보.html', model=weather,
                titles = ['기준일','기준시','카테고리','예보일','예보시','예보치','위도','경도'])

    def statics(self):
        statics = Service().staticsboard()
        matplotlib.use('agg')
        heatmap = Service().makeTempvsPressureHeatmap()
        return render_template('./dashboard/통계분석.html', model=statics, imgfile=heatmap)

    def inquery(self):
        linfo = Service().readLineInfo()
        #tables=[df.to_html(classes='female')],    titles = 'spark data'
        return render_template('./dashboard/데이터조회.html', model=linfo)

    def showpredict(self):
        YY = Service().predict()
        gettext('Please translate me, I am a message or some stuff!')
        return render_template('./dashboard/예측치보기.html', model=YY)

    def acquisition(self):
        linfo = Service().readLineInfo()
        return render_template('./model/데이터취득.html', model=linfo)     

    def eda(self):
        person = Service().addUser('HAKSOO', 'LIM')
        return render_template('./model/데이터정제.html', model=person)      

    def definemodel(self):
        person = Service().addUser('HAKSOO', 'LIM')
        return render_template('./model/모델정의.html', model=person)    

    def learning(self):
        history, imgfile = Service().study()
        return render_template('./model/학습하기.html', model=history, imgfile=imgfile)       

    def prediction(self):
        YY = Service().predict()
        return render_template('./model/예측하기.html', model=YY)        

    def gisinfo(self):
        person = Service().addUser('HAKSOO', 'LIM')
        return render_template('./user/지리정보입력.html', model=person)       

    def measureresult(self):
        person = Service().addUser('HAKSOO', 'LIM')
        return render_template('./user/측정결과입력.html', model=person)             

    def usermng(self):
        person = Service().addUser('HAKSOO', 'LIM')
        return render_template('./system/사용자정보관리.html', model=person)        

    def compmng(self):
        person = Service().addUser('HAKSOO', 'LIM')
        return render_template('./system/업체정보관리.html', model=person)       

    def clouderamng(self):
        person = Service().addUser('HAKSOO', 'LIM')
        return render_template('./system/클라우데라.html', model=person)    
    
                     




class Service:
    def addUser(self, firstname, lastname):
        person = Person(firstname, lastname)
        return person

    def readLineInfo(self):
        line_data = LineData(spark)
        df = line_data.getHiveInfo('jdbc:hive2://121.183.206.41:10000/test;',
            'HiveJDBC4.jar',
            "select * from default.fiberfox")
        return df

    def staticsboard(self):
        line_data = LineData(spark)
        df = line_data.getHiveInfo('jdbc:hive2://121.183.206.41:10000/test;',
            'HiveJDBC4.jar',
            "select * from default.fiberfox")
        return df        

    def makeTempvsPressureHeatmap(self):
        line_data = LineData(spark)
        lstdf = line_data.getHiveInfo('jdbc:hive2://121.183.206.41:10000/test;',
            'HiveJDBC4.jar',
            'select workdate, temperature, pressure, um from default.fiberfox')

        df = line_data.lstTodf(lstdf, columns=['workdate', 'temperature', 'pressure', 'um'], numbers=['temperature', 'pressure', 'um'])
        ddf = df.pivot_table(index='temperature',columns=['temperature', 'pressure'], values='um')

        sns.heatmap(ddf, cmap='YlGnBu') # 
        plt.title('temp. vs pressure', fontsize=20)
        plt.savefig('./static/img/TempvsPressure.png')
        return '/static/img/TempvsPressure.png'


    def weatherpredict(self):
        cdata = ClimateData(spark)
        now = datetime.datetime.now()
        nowDate = now.strftime('%Y%m%d')

        df = cdata.getREST("http://apis.data.go.kr/1360000/VilageFcstInfoService/getVilageFcst?serviceKey=iKk6l9e%2F8SmdYVCFKoRWVlgQHQ2F8hNtTAOiwOhu6LrXO507c3HqEr4P%2BaQwJVgESUzIHU5j63ajbyScF9pgXw%3D%3D&pageNo=1&numOfRows=100&dataType=XML&base_date=" + nowDate + "&base_time=0500&nx=1&ny=1&",2)
    
        ddf = cdata.lstTodf(df, columns=['fcstDate', 'fcstTime', 'nx', 'ny', 'category', 'fcstValue'], numbers=['fcstValue'])
        
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

        return ddfinal

    
    def study(self):
        data = Data(spark)
        
        df = data.mariaToSpark(spark, "jdbc:mysql://127.0.0.1:3306/test","fiberfox_raw", "root", "9907")

        X, Y = data.getXY(df)

        # 값, 단어 등의 피처를 담아 벡터로 만드는 임베딩 레이어의 데이터 갯수
        max_features = 1024
        batch_size = 1

        modelobj = Model()
        model = modelobj.getModel(max_features)

        # 모델 학습
        history = model.fit(X, Y, batch_size=batch_size, epochs=10)
        fname = modelobj.SaveStudyResult(history)

        # predict(model, df)
        return (history, fname)


    def predict(self):
        data = Data(spark)
        df = data.mariaToSpark(spark, "jdbc:mysql://127.0.0.1:3306/test","fiberfox_raw", "root", "9907")
        max_features = 1280
        modelobj = Model()
        model = modelobj.getModel(max_features)
        X, Y = data.getXY(df)
        
        # 추정 로스값 예측
        YY = model.predict(X)
        # print(YY)
        return YY        




if __name__=='__main__':
    app.debug=True
    
    controller = Controller()

    spark = SparkSession.builder.appName("fiberfox")\
    .config("spark.driver.extraClassPath",
         "C:/spark-3.0/jars/mariadb-java-client-1.1.10.jar")\
    .getOrCreate() 

    app.add_url_rule('/', '/start/main', lambda:controller.main())
    app.add_url_rule('/dashboard/weathermap', 'weathermap', lambda:controller.weathermap())
    app.add_url_rule('/dashboard/weatherpredict', 'weatherpredict', lambda:controller.weatherpredict())
    app.add_url_rule('/dashboard/statics', 'statics', lambda:controller.statics())
    app.add_url_rule('/dashboard/inquery', 'inquery', lambda:controller.inquery())
    app.add_url_rule('/dashboard/showpredict', 'showpredict', lambda:controller.showpredict())
    app.add_url_rule('/model/acquisition', 'acquisition', lambda:controller.acquisition())
    app.add_url_rule('/model/doeda', 'eda', lambda:controller.eda())
    app.add_url_rule('/model/definemodel', 'definemodel', lambda:controller.definemodel())
    app.add_url_rule('/model/learning', 'learning', lambda:controller.learning())
    app.add_url_rule('/model/prediction', 'prediction', lambda:controller.prediction())
    app.add_url_rule('/user/gisinfo', 'gisinfo', lambda:controller.gisinfo())
    app.add_url_rule('/user/measureresult', 'measureresult', lambda:controller.measureresult())    
    app.add_url_rule('/system/usermng', 'usermng', lambda:controller.usermng())
    app.add_url_rule('/system/compmng', 'compmng', lambda:controller.compmng())
    app.add_url_rule('/system/clouderamng', 'clouderamng', lambda:controller.clouderamng())       


    app.run()