#!/usr/bin/python3
import sys
import datetime

def check(j_content):
    if not all(i.isalpha() or i.isspace() for i in j_content['word']):
        return False

    # country code must consist of two upper case letters
    if ((len(j_content['countrycode']) != 2) or (j_content['countrycode'] != (j_content['countrycode']).upper())):
        return False

    # All values must be either true or false
    if not ((j_content['recognized'] == True) or (j_content['recognized'] == False)):
        return False

    # Numeric string that's 16 characters long 
    if (len(j_content['key_id']) != 16) or (not j_content['key_id'].isnumeric()):
        return False

    # There must be at least one stroke
    if (len(j_content['drawing']) < 1):
        return False

    # Flag will tell us whether the data was inconsistent in any of the strokes
    flag = False
    for stroke in j_content['drawing']:
        # Two lists only to store x and y values
        if len(stroke) != 2:
            flag = True
            break

        # The lengths of the two lists must be equal
        if ((len(stroke[0]) != len(stroke[1]))):
            flag = True
            break
    # In case something went wrong, the data point must be skipped
    if (flag):
        return False

    return True



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
            jcon = json.loads(line.strip())
            line=line.strip("{")

            line=line.strip("}")
            #print(word)
            attribute=line.split(",")
            day=get_day(attribute[2])
            if word==attribute[0].replace('"','').split(": ")[1] and attribute[3].replace('"','').split(": ")[1]=='false':
                if day in ['Saturday','Sunday']:
                    if (check(jcon)):
                        print('{0}\t{1}'.format("unrecognised",1))       
            else:
                if word==attribute[0].replace('"','').split(": ")[1]:
                    if attribute[3].replace('"','').split(": ")[1]=='true':
                        if (check(jcon)):
                            print('{0}\t{1}'.format("recognised",1))
            
    except:
        #print("exiting")
        sys.exit()


if  __name__=="__main__":
    word=sys.argv[1]
    mapper_task(word)
    


