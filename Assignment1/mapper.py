#!/usr/bin/python3
import sys
import datetime



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


            
def mapper_task(word):
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
                    print('{0}\t{1}'.format("unrecognised",1))       
            else:
                if word==attribute[0].replace('"','').split(": ")[1]:
                    if attribute[3].replace('"','').split(": ")[1]=='true':
                        print('{0}\t{1}'.format("recognised",1))
            
    except:
        #print("exiting")
        sys.exit()


if  __name__=="__main__":
    word=sys.argv[1]
    mapper_task(word)
    


