#mapper
import sys
import datetime




def mapper_task1(word):
    try:
        for line in sys.stdin:
            #if clean_code(line)!=True:
                #continue

            line=line.strip("{")
            line=line.strip("}")
            #print(word)
            attribute=line.split(",")
            #print(attribute[0])
            if word==attribute[0].replace('"','').split(": ")[1]:
                if attribute[3].replace('"','').split(": ")[1]=='true':
                    print('{0}\t{1}'.format(word,1))
    except:
        #print("exiting")
        sys.exit()   


def get_day(timestamp):
    #print(timestamp)
    value=timestamp.split(": ")[1]
    value=value.split(" ")[0]
    #print(value)
    value=value.replace('"','').split("-")
    year,month,day=(int(x) for x in value)
    #print(year,month,day)
    weekend=datetime.date(year, month, day) 
    return weekend.strftime("%A") 


            
def mapper_task2(word):
    try:
        for line in sys.stdin:
            #if clean_code(line)!=True:
                    #continue
            #print(line)
            line=line.strip("{")
            line=line.strip("}")
            #print(word)
            attribute=line.split(",")
            day=get_day(attribute[2])
            if word==attribute[0].replace('"','').split(": ")[1] and attribute[3].replace('"','').split(": ")[1]=='false':
                if day in ['Saturday','Sunday']:
                    print('{0}\t{1}'.format(word,1))            
            
    except:
        #print("exiting")
        sys.exit()


if  __name__=="__main__":
    word=input()
    mapper_task1(word)
    #mapper_task2(word)



