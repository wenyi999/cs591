from PythonApplication1 import Scan,Distinct,Map
from random import randint
import logging
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import lightgbm as lgb
import numpy

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

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
        x=[]
        y=[]
        for i in data_ETL:
            for j in range(0,len(i)):
                i[j]=int(i[j])
            x.append(i[0:len(i)-1])
            y.append(i[len(i)-1])
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.30, random_state=42)
        '''train_data = lgb.Dataset(data=x_train,label=y_train)
        test_data = lgb.Dataset(data=x_test,label=y_test)
        param = {'num_leaves': 31, 'num_trees':100, 'objective': 'binary'}
        param['metric'] = 'auc'
        num_round = 10
        bst = lgb.train(param, train_data, num_round, valid_sets=[test_data])
        bst.save_model('model.txt')'''
        
        x_train_array=numpy.array(x_train)
        x_test_array=numpy.array(x_test)
        y_train_array=numpy.array(y_train)
        y_test_array=numpy.array(y_test)
        params = {"n_jobs": 4, "n_estimators": 50,  "max_depth": 3}
        lgbm = lgb.LGBMClassifier(**params)
        lgbm.fit(x_train_array, y_train_array, eval_set=[(x_test_array, y_test_array)], eval_metric='auc')
        y_test_pred = lgbm.predict(x_test_array)
        report=classification_report(y_test_array, y_test_pred,target_names=None)
        logger.info(report)


