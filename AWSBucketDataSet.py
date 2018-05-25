
# coding: utf-8

# In[ ]:

#!/usr/bin/python


# In[1]:

# Parses dataset from an AWS bucket 
# D. Joachim, 5/23/18
#
# Input: file containing S3 credentials + file location:
#   line 1 = access key ID
#   line 2 = secret access key
#   line 3 = AWS bucket [sondefoobucket]
#   line 4 = AWS dataset directory [djoachim/data/2017/amzjaguar/]
#   line 5 = local directory for outputs
# 
# Output:
#   meta_full = all meta data
#   meta_file_list = file list


# In[3]:

import csv, sys, os, datetime 
import boto, json, math, sox
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
from timeit import default_timer as timer
import datetime, re


# In[278]:

infoFile = "/Users/joachimd/Desktop/dale_sonde/admin/ec2/myS3.txt"
dataInfo = [line.rstrip('\n') for line in open(infoFile)]


# In[284]:

# accommodate command line argument for info file location

if len(sys.argv) > 1:
    print sys.argv[1]
    infoFile = str(sys.argv[1])


# In[254]:

tmp_file = '/tmp/' + 'tmp.wav'
minimumAudioFileSize = 1.0 * 44100


# In[5]:

# establish S3 connection
conn = boto.connect_s3(dataInfo[0], dataInfo[1])

# read specific S3 bucket and directory with
bucket = conn.get_bucket(dataInfo[2])
file_handles = [f for f in bucket.list(dataInfo[3], "/")  
                if len(os.path.basename(f.key))>0]
full_paths = [str(f.key) for f in file_handles]
file_names = pd.Series([os.path.basename(w) for w in full_paths])


# In[187]:

# organize as dataframe
df_fnames = pd.DataFrame(dict(
    is_wav = file_names.str.endswith('wav'),
    is_meta = file_names.str.endswith('meta'),
    is_answer = file_names.str.endswith('answers'),
    unique_id = file_names.str.split('.').apply(lambda x: x[0]),
    file_handle = file_handles,
    file_size = [h.size for h in file_handles],
    full_path = full_paths,
    Wave_Name = file_names,
    Wave_Dir = [os.path.dirname(w) for w in full_paths],
    Data_Set = [os.path.dirname(w).split("/")[-1].upper() for w in full_paths]
))


# In[188]:

# read meta json from AWS
def load_meta(row):
    if (row.is_meta | row.is_answer ) == True:
        # print row.file_handle.key
        return json.loads(row['file_handle'].read())  
    else:
        return None


# In[189]:

# load data from meta and answer files
# file_handles aligned with dataframe idx
# 85.68 secs minutes for AMZJAGUAR

s = timer()
meta_sel_flags = (df_fnames.is_meta | df_fnames.is_answer ) == True
df_fnames['meta_data'] = df_fnames.apply(load_meta, axis=1)
meta_files = pd.DataFrame(df_fnames.loc[meta_sel_flags])
sys.stdout.write("Load meta_data time: %5.2f s:\n" % (timer()-s))


# ### Function: compute PHQ9 Scores

# In[190]:

def compute_phq9score(q_dict):    
    score = -1
    if q_dict != None:
        select_keys = ['test_type','questionnaire_end_time','questionnaire_id',
                       'questionnaire_start_time']
        t = json_normalize(q_dict,'question_responses',select_keys)
        tt = t[t.test_type.apply(lambda x: x[:3]=='PHQ') & 
               np.logical_not(t.user_did_skip) & (t.question_number < 10)]
        score = sum(tt.score)
    return score


# ### Ingest questionaire data

# In[191]:

meta_files.loc[:,'User_ID'] = 'None'
meta_files.loc[:,'PHQ9_Score'] = -1
meta_files['q_flag'] = False
meta_files['DoB'] = None
meta_files['audio_exists'] = True
meta_files['Wave_Time'] = None

s = timer()

# the next 2 loops purposely separately executed

for m_idx in meta_files.index:
    m_data = meta_files.meta_data[m_idx]
    meta_files.loc[m_idx,'User_ID'] = m_data['user_id']
    sys.stdout.write("\rRaw audio data %d: %5.2f mins" % (m_idx,(timer()-s)/60.0))

scores = []
for m_idx in meta_files.index:
    m_data = meta_files.meta_data[m_idx]
    if 'questionnaire_data' in m_data:        
        meta_files.loc[m_idx,'q_flag'] = True
        score = compute_phq9score(m_data['questionnaire_data'])
        same_user_idx = (meta_files['User_ID'] == m_data['user_id'])
        print '*',; scores += [[score,sum(same_user_idx)]]
        # tag same user_id rows with PHQ9 score 
        meta_files.loc[same_user_idx,'PHQ9_Score'] = score
        sys.stdout.write("\rQuestionnaire data %d: %5.2f mins" % (m_idx,(timer()-s)/60.0)) 
    else:        
        d = dict(DoB = None, Date_Time = None)
        if 'date_of_birth' in m_data:
            meta_files.loc[m_idx,['DoB','Wave_Time']] = [m_data['date_of_birth'], 
                    m_data['device_current_date_time']]
        
meta_files.loc[:,'PHQ9_Score_Ok'] = (meta_files['PHQ9_Score'] != -1)
meta_files.loc[:,'Meta_File_Exists'] = True
meta_files.loc[:,'Landmark_Exists'] = True
meta_files.loc[:,'Meta_Seconds'] = None
meta_files.loc[:,'Json_Name'] = meta_files['Wave_Name']


# ### Basic stats

# In[192]:

if False:
    t = ['questionnaire_data' in meta_files.meta_data[m_idx] for m_idx in meta_files.index]
    print 'Number of Meta files ->',len(meta_files)
    print 'Users with PHQ9 scores ->', sum(t)
    xx = [x[1] for x in scores]
    print 'Number of PHQ9 tagged files ->', sum(xx)
    print 'Task Count: occurrences ->', {x:xx.count(x) for x in set(xx)}


# In[193]:

if False:
    w = meta_files.loc[meta_files.is_meta,'PHQ9_Score'].value_counts()
    print "Meta files without PHQ9: ", w[-1]
    print "Meta files with PHQ9 < 10: ", sum(w[i] for i in w.index if (i > -1) & (i < 10))
    print "Meta files with PHQ9 > 9: ", sum(w[i] for i in w.index if (i > 9))
    print "Files with questionnaire data: ", sum(['questionnaire_data' in m_data for m_data in meta_files.meta_data])
    print "Total files with PHQ9 -> ", sum(w) 


# ### Task labels

# In[198]:

# note that activity is set to None for 'check_in' and 'check_in_remote'
def update_activity(m):

    # lower case for parsing
    activity = m['activity'].lower()
    if 'sub_activity' in m:
        sub_activity = m['sub_activity'].lower()
    else:
        sub_activity = 'None'

    # activity mapping
    if (activity == 'baseline') & (sub_activity == 'pa_ta_ka'):
        activity, sub_activity = 'Word', 'Pataka'
    elif (activity == 'baseline') & (sub_activity == 'ahhh'):
        activity, sub_activity = 'Utterance', 'AH'
    elif (activity == 'baseline') & (sub_activity == 'short_reading'):
        activity, sub_activity = 'Passage', 'ShortReading'
    elif (activity == 'free_speech') | (activity == 'freespeech'):
        activity, sub_activity = 'FreeSpeech', sub_activity.title()
    elif (activity == 'passage reading') | (activity == 'passage_reading') | (activity == 'shortreading'):
        activity, sub_activity = 'Passage', sub_activity.title()
    elif (activity == 'focus'):
        activity, sub_activity = 'CL', sub_activity.title()
    elif (activity == 'memory'):
        activity, sub_activity = 'Memory', sub_activity.title()
    elif (activity == 'ahhh'):
        activity, sub_activity = 'Utterance', 'AH'
    elif (activity == 'pataka'):
        activity, sub_activity = 'Word', 'Pataka'
    elif (activity == 'sentence_reading') | (activity == 'sentencereading'):
        activity, sub_activity = 'Sentence', sub_activity.title()
    elif (activity == 'check_in_remote') | (activity == 'check_in'):
        activity, sub_activity = 'None', None

    m['activity'] = activity 
    m['sub_activity'] = sub_activity
    return m

sel_dict = {u'device_model':'Model', u'gender':'Gender',
 u'manufacturer':'Manufacturer',u'recording_duration':'Duration',u'system_version':'OS',
 u'activity':'Task', u'sub_activity':'Sub_Task', # u'unique_id':'Wave_Name',
 u'sample_rate':'SampleRate', 'file_handle':'FileHandle','file_size':'AWS_File_Size',
 u'user_id': 'Person_ID', u'end_time': 'Wave_Time', 
 'audio_exists':'Wave_File_Exists', u'date_of_birth': 'DoB', }

sel_other = ['PHQ9_Score','PHQ9_Score_Ok','Meta_File_Exists',
         'Landmark_Exists','Meta_Seconds','Json_Name','Data_Set',
            'Wave_Name', 'Wave_Dir','is_meta']

# consolidate samplerates with multiple keys 
meta_list = [{(u'sample_rate' if 'ample' in k else k):v for k,v in m_data.items()} 
                for m_data in meta_files.meta_data]

meta_list = [{k:v for k,v in m_data.items() 
              if k in sel_dict.keys()+sel_other} 
                for m_data in meta_list]


no_activity = [m for m in meta_list if 'activity' not in m]
yes_activity = [m for m in meta_list if 'activity' in m]
                                                                              
a_sel = [x['manufacturer'] for x in no_activity]
t = {x:a_sel.count(x) for x in set(a_sel)}
#print "Meta entries without activity: ", t, "\n"

a = 'activity'
list_a = [update_activity(x)[a] for x in yes_activity]
#print "%s count: " % a, {x:list_a.count(x) for x in set(list_a)}


# ### Additional cleaning

# In[199]:

df = pd.concat([meta_files.reset_index(),pd.DataFrame(meta_list)],axis=1)

# exclude non .meta file entries
m_df = df.loc[df.is_meta][sel_dict.keys()+sel_other]

m_df = m_df.rename(columns=sel_dict)
m_df['Wave_Name'] = m_df['Wave_Name'] + '.wav'
m_df['Meta_Time'] = m_df['Wave_Time']  # legacy

# uppercase first character
for cat in ['Model', 'Gender','Manufacturer','OS']:
    m_df[cat] = m_df[cat].str.title() 



# ### Remove rows corresponding to NOAUDIO files

# In[245]:

meta_df = m_df 
noaudio_list = [x.split('.')[0] for x in file_names if "NOAUDIO" in x]
noaudio_flags = pd.concat(
    [meta_df.Wave_Name.str.contains(x) for x in noaudio_list], axis = 1).any(axis=1)
meta_df = meta_df.loc[~noaudio_flags]


# ### Update audio file size (from meta file to audio file)

# In[255]:

# one sec minimum!!

f_sizes = []
for f in meta_df.Wave_Name:
    chk = df_fnames.Wave_Name.str.contains(f.split('.')[0])         & df_fnames.Wave_Name.str.contains('wav')
    f_sizes = f_sizes +  [df_fnames.loc[chk].file_size.values[0]]
    
meta_df.loc[:,'AWS_File_Size'] = f_sizes
meta_df = meta_df.loc[meta_df.AWS_File_Size > minimumAudioFileSize]


# ### Read each file into tmp, then extract sox params

# In[174]:

# ---------------------------------------------------------
# DJ's interface to SoX. Consider using pySoX in the future
# function: sox2stats
#
# Function to extract audio stats from file.
# It uses the command line utility 'Sox'
# The return tuple includes the original index
# in order to correctly place result during parallel processing
# suppress warnings (premature EOF ...)
# ---------------------------------------------------------

def sox2stat2 (full_name):

    t = os.popen("/usr/local/bin/sox -V1 " + "%s"%full_name + " -n stat 2>&1")
    a = pd.DataFrame([x.split(': ') for x in t.read().split('\n')][0:15])
    t = os.popen("/usr/local/bin/soxi " + "%s"%full_name + " 2>&1")
    b = pd.DataFrame([x.split(': ') for x in t.read().split('\n')][1:9])
    info = a.append(b,ignore_index = True)
    info.columns = ['Parameter','Data']
    info.set_index('Parameter', drop=True, inplace=True)

    info = info.transpose()
    info.columns = [x.strip() for x in info.columns]

    # drop unwanted columns, rename columns

    info = info.drop(['Duration','Input File'],1)
    info.columns = [re.sub('[\ ()]', '',x.title()) for x in list(info)]

    for c in info.columns:
        val = info[c].values
        if c not in ['SampleEncoding','BitRate','FileSize','Precision']:
            try:
                info[c] = pd.to_numeric(val)
            except:
            # print full_name
            # print info
                return pd.DataFrame()

    return info


# In[260]:

# compute SoX parameters
# amzjaguar (387 files) in 268 sec

s = timer()
sox_df = pd.DataFrame()
for idx, row in meta_df.iterrows():
    srcFileName = row.Wave_Dir + '/' + row.Wave_Name
    k = boto.s3.key.Key(bucket,srcFileName)
    k.get_contents_to_filename(tmp_file)
    sox_df = pd.concat([sox_df,sox2stat2(tmp_file)],axis=0)
    
sys.stdout.write("SoX parameter extraction time: %5.2f s:\n" % (timer()-s))


# ### Merge meta + sox data, then create and store two dataframes

# In[276]:

sox_df.reset_index(drop=True, inplace=True)
meta_df.reset_index(drop=True, inplace=True)
data_df = pd.concat([meta_df, sox_df], axis=1)
data_df = data_df.drop(columns = ['FileHandle','is_meta'])


# In[281]:

data_df.to_csv(dataInfo[4]+'full_data_set.csv')
data_df[['Wave_Dir','Wave_Name']].to_csv(dataInfo[4]+'files_data_set.csv')

