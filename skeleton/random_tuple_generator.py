from random import randint

ei=randint(1, 241);bi=0;  
fd = open('D:\click_ad_data\train.csv', 'r');  
ofd = open('../data/sample.csv', 'w');  
while True:  
    line = fd.readline();  
    if not line:  
        break;  
    bi += 1;  
    if bi != ei:  
        continue;  
    ofd.write(line);  
    bi = 0;  
    ei = randint(1, 241)  