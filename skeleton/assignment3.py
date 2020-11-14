from assignment_12 import Scan,Distinct,Map
from random import randint
import logging
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import lightgbm as lgb
import numpy
import sklearn
import sklearn.datasets
import sklearn.ensemble
import lime
import lime.lime_tabular
import tkinter
import matplotlib

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

matplotlib.use('TkAgg')

if __name__ == "__main__":
        ans_list=[]
        att_list=["ID","datetime","siteid","offerid","category","merchant","countrycode","browserid","devid","click"]
        for i in range(0,10):
            scan_data=Scan('../data/sample.csv',None,False,False)
            distinct=Distinct(scan_data,i,False,False)
            ans=distinct.get_next()
            if("empty" in ans):
                logger.info("There are "+str(ans["empty"])+" missing values for the attribute "+att_list[i])
            else:
                logger.info("There are no missing values for the attribute "+att_list[i])
            if(i==6 or i==7 or i==8):
                logger.info("The distinct values for attribute "+att_list[i]+" are shown below,")
                logger.info(ans)
                ans_list.append(ans)
        keys={}
        for i in ans_list:
            index=1
            keys_of_one_attr=i.keys()
            for j in keys_of_one_attr:
                if j=="empty":
                    keys[j]=0
                else:
                    keys[j]=index
                    index+=1
        scan_data=Scan('../data/sample.csv',None,False,False)
        map_data=Map(scan_data,keys)
        data_ETL=map_data.get_next()
        logger.info(data_ETL[0])

        #task 3
        x=[]
        y=[]
        for i in data_ETL:
            for j in range(0,len(i)):
                i[j]=int(i[j])
            x.append(i[0:len(i)-1])
            y.append(i[len(i)-1])
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.30, random_state=42)        
        x_train_array=numpy.array(x_train)
        x_test_array=numpy.array(x_test)
        y_train_array=numpy.array(y_train)
        y_test_array=numpy.array(y_test)
        params = {"n_jobs": 4, "n_estimators": 50,  "max_depth": 3}
        lgbm = lgb.LGBMClassifier(**params)
        lgbm.fit(x_train_array, y_train_array, eval_set=[(x_test_array, y_test_array)], eval_metric='auc')
        y_test_pred = lgbm.predict(x_test_array)
        report=classification_report(y_test_array, y_test_pred,target_names=None)
        logger.info("\n"+report)

        #task 4
        scan2=Scan('../data/sample_for_task4.csv',None,False,False)
        map2=Map(scan2,keys)
        data2=map2.get_next()
        x2=[]
        y2=[]
        for i in data2:
            for j in range(0,len(i)):
                i[j]=int(i[j])
            x2.append(i[0:len(i)-1])
            y2.append(i[len(i)-1])
        x2_array=numpy.array(x2)
        y2_array=numpy.array(y2)

        logger.info(sklearn.metrics.accuracy_score(y_test_array, lgbm.predict(x_test_array)))
        feature_names=["month","day","hour","minute","siteid","offerid","category","merchant","countrycode","browserid","devid","click"]
        target_names=["not click","click"]
        explainer = lime.lime_tabular.LimeTabularExplainer(x_train_array, "classification",y_train_array,feature_names,categorical_features=None, categorical_names=None, kernel_width=None, verbose=False, class_names= target_names, feature_selection='auto', discretize_continuous=False)
        exp = explainer.explain_instance(x2_array[0], lgbm.predict_proba, num_features=11)#, top_labels=1)
        exp.show_in_notebook(show_table=True, show_all=False)
        exp.as_pyplot_figure()
        exp = explainer.explain_instance(x2_array[1], lgbm.predict_proba, num_features=11, top_labels=1)
        exp.show_in_notebook(show_table=True, show_all=False)
        #exp.as_pyplot_figure()





