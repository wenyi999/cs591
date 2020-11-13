from PythonApplication1 import Scan,Distinct,Map
from random import randint
import logging
from sklearn.model_selection import train_test_split

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
        x_train, x_test, y_train, y_test = train_test_split(
    x, y, test_size=0.30, random_state=42)
        #logger.info(x_train[0])


