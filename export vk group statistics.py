#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
from datetime import date
from datetime import time

import time
import numpy as np
import re

id_group = 'polisorb'                                                               # Ссылка на группу
token = 'af567951af567951af5679518caf3e98dfaaf56af567951f3270969be965f073f20981f'                                                              # вставить токен ВК
count_posts = 800                                                    # количество постов для анализа
brand_name = ['Контур'] # Брендовые запросы


# In[3]:


offset = 0      
count = 100       
data_posts = []

while offset < count_posts:

    url = 'https://api.vk.com/method/wall.get'
    params = {
        'domain': id_group,
        'filter': 'owner',   #owner - посты только от владельца, all = все посты, others - гостевые посты
        'count': count,
        'offset': offset,
        'access_token': token,
        'v': 5.73,
        'verify': False
    }
    
    url_comments = 'https://api.vk.com/method/wall.getComments'
    
    r = requests.get(url, params = params).json()
    
    check = True
    if check:
        real_count = r['response']['count']
        if real_count < count_posts:
            count_posts = real_count
            check = False
        else:
            check = False
    
    data_posts += r['response']['items'] 
    offset += count    
    time.sleep(0.5)


# In[ ]:


stats = []

for record in data_posts:   
    title = record['text']               # Текст поста
    if len(title) > 10:                                 #Берем первые 1000 символов из названия поста
        title = title[:1000]
    
    len_title = len(record['text'])
    if len_title < 100: # Рассчитываем длину поста
        len_title = len_title // 10 * 10
    else:
        len_title = len_title // 100 * 100
        
    date = datetime.fromtimestamp(record['date']).strftime('%Y-%m-%d')
    hour = datetime.fromtimestamp(record['date']).strftime('%H')
    day_of_week = datetime.fromtimestamp(record['date']).strftime('%A')
    day = datetime.fromtimestamp(record['date']).strftime('%d')
    month = datetime.fromtimestamp(record['date']).strftime('%m')
    year = datetime.fromtimestamp(record['date']).strftime('%Y')
    full_date = datetime.fromtimestamp(record['date']).strftime('%Y-%m-%d-%H:%M')
  
    attachment = {'photo': 0, 'audio': 0, 'video': 0 , 'link': 0, 'poll': 0, 'doc': 0, 'app': 0, 'page': 0, 'album': 0}            # Взять можно отсюда https://vk.com/dev/objects/attachments_w
        
    if 'attachments' in record:                                                          # цикл для подсчета типов и кол-ва вложений
        for attach in record['attachments']:
            if attach['type'] in attachment:
                attachment[attach['type']] = attachment[attach['type']] + 1
    
    photo_link = []
    if 'attachments' in record:                                                          # цикл для подсчета типов и кол-ва вложений
        for attach in record['attachments']:
            if attach['type'] == 'photo':
                for key, p in attach['photo'].items():
                    if key == 'photo_807':
                        photo_link.append(p)
    photo_link = str(photo_link).strip('['']').replace("'", "")
    photo_link = photo_link.split(',')[0]
    
    if 'views' in record:
        views = record['views']['count']
    else:
        views = 0
    
    total_actions = record['comments']['count'] + record['likes']['count'] + record['reposts']['count']    #сумируем все активности 
    
    engagement_rate_total = total_actions / views
    engagement_rate_likes = record['likes']['count'] / views
    engagement_rate_comments = record['comments']['count'] / views
    engagement_rate_reposts = record['reposts']['count'] / views
    
    #создаем список и добавляем в него название, длину, кол-во фото, кол-во аудио, кол-во видео в постах, постов с ссылками, пстов с опросами, просмотры, кол-во просмотров, комментариев, лайков, репостов, сумму всех взаимодействий, дату и час  
    stats.append([title, len_title, photo_link, attachment['photo'], attachment['audio'], attachment['video'], attachment['link'], attachment['poll'], attachment['doc'], attachment['app'], attachment['page'], attachment['album'], views , record['comments']['count'], record['likes']['count'], record['reposts']['count'], total_actions, engagement_rate_total, engagement_rate_likes, engagement_rate_comments, engagement_rate_reposts, full_date, date, hour, day_of_week, day, month, year])

#Создаем DataFrame (таблицу) из данных и записываем
columns = ["name_post", 'len_text', 'photo_link', 'photo', 'audio', 'video', 'link', 'poll', 'doc', 'app', 'page', 'album', "views", "comments", "likes", "share", 'total_action', "engagement total", "engagement likes", "engagement comments", "engagement reposts", 'full_date', "date", "hour", "day of week", "day", "month", "year"] #задаем заголовки таблицы
df = pd.DataFrame(data=stats, columns=columns)


# In[ ]:


# Поиск чисел в тексте поста
def find_numbers(row):
    result = 0
    if re.findall('\d+', row['name_post']):
        result = 1
    else:
        result = 0
    return result
df['numbers_in_post'] = df.apply(find_numbers, axis=1)


# In[ ]:


text = " ".join(post for post in df.name_post)


# In[ ]:


# Расчет статистики использования символов и влияния на er
df_symbols = pd.DataFrame([',', '.', '?', '!', ':', '+', '-'], columns = ['symbol'])

# Популярность хэштегов
hashtags = [w for w in text.split() if w.startswith('#')]
hashtags = [item.replace('!','').replace('.','').replace(',','').replace(';','').replace("'",'').replace(":",'') for item in hashtags]
hashtags_unique = set(hashtags)

df_hashtags = pd.DataFrame(hashtags_unique)
df_hashtags.columns = ['hashtag']

# Упоминание бренда
df_brand = pd.DataFrame(brand_name)
df_brand.columns = ['brand']


# In[ ]:


# Методы для расчета частоты упоминания и основных метрик вовлечения
def posts_number(row, column):
    result = 0
    for post in df['name_post']:
        if row[column] in post:
            result += 1
        else:
            result += 0
    return result

def posts_statistics(row, column, plus):
    result = 0
    n = 0
    for post in df['name_post']:
        if row[column] in post:
            result += df[plus][n]
        else:
            result += 0
        n+=1
    return result


# In[ ]:


# Группировка таблиц по часам и удаление ненужных столбцов
df_hour = df.drop(['len_text', 'photo', 'audio', 'video', 'link', 'poll', 'doc', 'app', 'page', 'album', 
                   'numbers_in_post', 'engagement total', 'engagement likes', 'engagement comments', 
                   'engagement reposts'], axis=1)
df_group_by_hour = df_hour.groupby('hour').sum()                                                          #группируем значения по часу
df_group_by_hour['count_post'] = df_hour.groupby('hour')['name_post'].count()                             #считаем колличество постов вышедших в данный час
df_group_by_hour['mean_action'] = df_group_by_hour['total_action'] /df_group_by_hour['count_post']        #считаем среднее значение активности (все активности / кол-во активностей)
df_group_by_hour['views_on_post'] = df_group_by_hour['views'] / df_group_by_hour['count_post']
df_group_by_hour['er'] = df_group_by_hour['total_action'] / df_group_by_hour['views'] #считаем ER (все активности / кол-во просмотров * 100)
df_group_by_hour = df_group_by_hour.sort_values(by="hour", ascending=True)                             

# Группировка таблиц по типам и удаление ненужных столбцов
df_type = df.drop(['date', 'hour', 'day of week', 'day', 'month', 'year', 'engagement total', 'engagement likes', 
                   'numbers_in_post', 'engagement comments', 'engagement reposts'], axis=1)
df_group_by_len_title = df_type.groupby('len_text').sum()
df_group_by_len_title['count_posts'] = df_type.groupby('len_text')['name_post'].count()
df_group_by_len_title['mean_action'] = df_group_by_len_title['total_action'] / df_group_by_len_title['count_posts']
df_group_by_len_title['views_on_post'] = df_group_by_len_title['views'] / df_group_by_len_title['count_posts']
df_group_by_len_title['er'] = df_group_by_len_title['total_action'] / df_group_by_len_title['views']
df_group_by_len_title = df_group_by_len_title.sort_values(by='len_text', ascending=True)
df_group_by_len_title = df_group_by_len_title.style.format("{:.2f}")

df_day_of_week = df.drop(['len_text', 'photo', 'audio', 'video', 'link', 'poll', 'doc', 'app', 'page', 'album', 
                          'numbers_in_post', 'engagement total', 'engagement likes', 'engagement comments', 
                          'engagement reposts'], axis=1)
df_group_by_day_of_week = df_day_of_week.groupby('day of week').sum()
df_group_by_day_of_week['count_post'] = df_day_of_week.groupby('day of week')['name_post'].count()                             #считаем колличесво постов вышедших в данный час
df_group_by_day_of_week['mean_action'] = df_group_by_day_of_week['total_action'] /df_group_by_day_of_week['count_post']        #считаем среднее значение активности (все активности / кол-во активностей)
df_group_by_day_of_week['views_on_post'] = df_group_by_day_of_week['views'] / df_group_by_day_of_week['count_post']
df_group_by_day_of_week['er'] = df_group_by_day_of_week['total_action'] / df_group_by_day_of_week['views'] #считаем ER (все активности / кол-во просмотров * 100)
df_group_by_day_of_week = df_group_by_day_of_week.sort_values(by="day of week", ascending=True)  

# Группировка день недели + час
df_week_with_hour = df.drop(['len_text', 'photo', 'audio', 'video', 'link', 'poll', 'doc', 'app', 'page', 
                             'numbers_in_post', 'album', 'engagement total', 'engagement likes', 'engagement comments', 
                             'engagement reposts'], axis=1)

df_week_with_hour['er'] = df_week_with_hour['total_action'] / df_week_with_hour['views']
df_week_with_hour['er_likes'] = df_week_with_hour['likes'] / df_week_with_hour['views']
df_week_with_hour['er_shares'] = df_week_with_hour['share'] / df_week_with_hour['views']
df_week_with_hour['er_comments'] = df_week_with_hour['comments'] / df_week_with_hour['views']

df_group_by_day_of_wh = df_week_with_hour.pivot_table(index='day of week', columns='hour', values='er', aggfunc=np.mean)
df_reach = df_week_with_hour.pivot_table(index='day of week', columns='hour', values='views', aggfunc=np.mean)
df_likes = df_week_with_hour.pivot_table(index='day of week', columns='hour', values='er_likes', aggfunc=np.mean)
df_shares = df_week_with_hour.pivot_table(index='day of week', columns='hour', values='er_shares', aggfunc=np.mean)
df_comments = df_week_with_hour.pivot_table(index='day of week', columns='hour', values='er_comments', aggfunc=np.mean)

df_group_by_day_of_wh = df_week_with_hour.pivot_table(index='day of week', columns='hour', values='er', aggfunc=np.mean)

# Группировка по количеству постов в день
posts_per_day = df.groupby('date')['name_post'].count()
df_ppd = df.merge(posts_per_day, on='date')
df_ppd = df_ppd.drop(['len_text', 'date', 'hour', 'day of week', 'day', 'month', 'year', 'numbers_in_post', 'engagement total', 
                      'engagement likes', 'engagement comments', 'engagement reposts'], axis=1)
df_ppd=df_ppd.rename(columns = {'name_post_y':'posts per day count'})
df_posts_per_day = df_ppd.groupby('posts per day count').sum()
df_posts_per_day['count_posts'] = df_ppd.groupby('posts per day count')['name_post_x'].count()
df_posts_per_day['mean_action'] = df_posts_per_day['total_action'] / df_posts_per_day['count_posts']
df_posts_per_day['views_on_post'] = df_posts_per_day['views'] / df_posts_per_day['count_posts']
df_posts_per_day['er'] = df_posts_per_day['total_action'] / df_posts_per_day['views']
df_posts_per_day = df_posts_per_day.sort_values(by='posts per day count', ascending=True)
df_posts_per_day = df_posts_per_day.style.format("{:.2f}")

df_attach_type = pd.DataFrame(columns=['photo','audio', 'video','link','poll', 'doc', 'app', 'page', 'album'], index=['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11','12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23'])
for i in ['photo','audio', 'video','link','poll', 'doc', 'app', 'page', 'album']:
    views_photo = df[df[i] != 0].groupby('hour')['views'].sum()
    activity_photo = df[df[i] != 0].groupby('hour')['total_action'].sum()
    df_attach_type = df_attach_type.join(pd.DataFrame(activity_photo / views_photo, columns=[i]), lsuffix='_left', rsuffix='_right')

df_attach_type=df_attach_type.rename(columns = {'photo_right': 'photo', 'audio_right': 'audio', 
                                                'video_right': 'video', 'link_right': 'link', 
                                                'poll_right': 'poll', 'doc_right': 'doc', 
                                                'app_right': 'app', 'page_right': 'page', 
                                                'album_right': 'album'})
df_attach_type = df_attach_type[['photo','audio', 'video','link','poll', 'doc', 'app', 'page', 'album']]
# Группировка содержание + время, содержание + день недели

df_length_with_hour = df[['total_action', 'views', 'len_text', 'hour']]
df_length_with_hour['er'] = df_length_with_hour['total_action'] / df_length_with_hour['views']

df_length_with_hour_pt = df_length_with_hour.pivot_table(index='len_text', columns='hour', values='er', aggfunc=np.mean)

df_numbers = df.drop(['date', 'hour', 'day of week', 'day', 'month', 'year', 'engagement total', 'engagement likes', 'engagement comments', 'engagement reposts'], axis=1)
df_posts_with_numbers = df_numbers.groupby('numbers_in_post').sum()
df_posts_with_numbers['count_posts'] = df_numbers.groupby('numbers_in_post')['name_post'].count()
df_posts_with_numbers['mean_action'] = df_posts_with_numbers['total_action'] / df_posts_with_numbers['count_posts']
df_posts_with_numbers['views_on_post'] = df_posts_with_numbers['views'] / df_posts_with_numbers['count_posts']
df_posts_with_numbers['er'] = df_posts_with_numbers['total_action'] / df_posts_with_numbers['views']
df_posts_with_numbers = df_posts_with_numbers.sort_values(by='len_text', ascending=True)
df_posts_with_numbers = df_posts_with_numbers.style.format("{:.2f}")

# Расчет статистики по хештегам
df_hashtags['count_posts'] = df_hashtags.apply(posts_number, args = ['hashtag'], axis=1)
df_hashtags['views'] = df_hashtags.apply(posts_statistics, args = ['hashtag', 'views'], axis=1)
df_hashtags['comments'] = df_hashtags.apply(posts_statistics, args = ['hashtag', 'comments'], axis=1)
df_hashtags['likes'] = df_hashtags.apply(posts_statistics, args = ['hashtag', 'likes'], axis=1)
df_hashtags['share'] = df_hashtags.apply(posts_statistics, args = ['hashtag', 'share'], axis=1)
df_hashtags['total_action'] = df_hashtags.apply(posts_statistics, args = ['hashtag', 'total_action'], axis=1)
df_hashtags['mean_actions'] = df_hashtags['total_action'] / df_hashtags['count_posts']
df_hashtags['er'] = df_hashtags['total_action'] / df_hashtags['views']
df_hashtags = df_hashtags.sort_values(by='er', ascending=False)

# Расчет статистики по символам
df_symbols['count_posts'] = df_symbols.apply(posts_number, args = ['symbol'], axis=1)
df_symbols['views'] = df_symbols.apply(posts_statistics, args = ['symbol', 'views'], axis=1)
df_symbols['comments'] = df_symbols.apply(posts_statistics, args = ['symbol', 'comments'], axis=1)
df_symbols['likes'] = df_symbols.apply(posts_statistics, args = ['symbol', 'likes'], axis=1)
df_symbols['share'] = df_symbols.apply(posts_statistics, args = ['symbol', 'share'], axis=1)
df_symbols['total_action'] = df_symbols.apply(posts_statistics, args = ['symbol', 'total_action'], axis=1)
df_symbols['mean_actions'] = df_symbols['total_action'] / df_symbols['count_posts']
df_symbols['er'] = df_symbols['total_action'] / df_symbols['views']
df_symbols = df_symbols.sort_values(by='er', ascending=False)

# Расчет статистики по брендовым запросам
df_brand['count_posts'] = df_brand.apply(posts_number, args = ['brand'], axis=1)
df_brand['views'] = df_brand.apply(posts_statistics, args = ['brand', 'views'], axis=1)
df_brand['comments'] = df_brand.apply(posts_statistics, args = ['brand', 'comments'], axis=1)
df_brand['likes'] = df_brand.apply(posts_statistics, args = ['brand', 'likes'], axis=1)
df_brand['share'] = df_brand.apply(posts_statistics, args = ['brand', 'share'], axis=1)
df_brand['total_action'] = df_brand.apply(posts_statistics, args = ['brand', 'total_action'], axis=1)
df_brand['mean_actions'] = df_brand['total_action'] / df_brand['count_posts']
df_brand['er'] = df_brand['total_action'] / df_brand['views']
df_brand = df_brand.sort_values(by='er', ascending=False)

#запись в excel файл 
with pd.ExcelWriter('data_vk_{}.xlsx'.format(id_group)) as writer:                                     
    df.to_excel(writer, index = False , sheet_name='Исходный DataFrame')
    df_group_by_hour.to_excel(writer, index = True, sheet_name='Группировка по часу')
    df_group_by_len_title.to_excel(writer, index = True, sheet_name='Группировка по кол-ву символов')
    df_group_by_day_of_week.to_excel(writer, index = True, sheet_name='Группировка по дням недели')
    df_posts_per_day.to_excel(writer, index = True, sheet_name='Групп. по кол-ву сообщ. в день')
    df_group_by_day_of_wh.to_excel(writer, index = True, sheet_name='ER по дням нед. и час.')
    df_attach_type.to_excel(writer, index = True, sheet_name='ER по типу вложения и времени')
    df_length_with_hour_pt.to_excel(writer, index=True, sheet_name='ER по длине сообщения и времени')
    df_posts_with_numbers.to_excel(writer, index=True, sheet_name = 'ER по наличию чисел в тексте')
    df_hashtags.to_excel(writer, index=False, sheet_name='ER по хэштегам')
    df_symbols.to_excel(writer, index=False, sheet_name='ER по символам')
    df_brand.to_excel(writer, index=False, sheet_name='ER по упоминанию бренда')
    df_reach.to_excel(writer, index=True, sheet_name='Ср. охват по времени и дню нед')
    df_likes.to_excel(writer, index=True, sheet_name='ER лайки по врем. и дню нед')
    df_shares.to_excel(writer, index=True, sheet_name='ER репосты по врем. и дню недд')
    df_comments.to_excel(writer, index=True, sheet_name='ER комменты по врем. и дню нед')
    for atach in ['photo','audio', 'video','link','poll', 'doc', 'app', 'page', 'album']:
        df_group_by_temp = df_type.groupby(atach).sum()
        df_group_by_temp = df_group_by_temp.loc[:,["views", "comments", "likes", "share", 'total_action']]
        df_group_by_temp['count_posts'] = df_type.groupby(atach)['name_post'].count()
        df_group_by_temp['mean_action'] = df_group_by_temp['total_action'] / df_group_by_temp['count_posts']
        df_group_by_temp['views_on_post'] = df_group_by_temp['views'] / df_group_by_temp['count_posts']
        df_group_by_temp['er'] = df_group_by_temp['total_action'] / df_group_by_temp['views']
        df_group_by_temp = df_group_by_temp.sort_values(by='er', ascending=False)
        df_group_by_temp = df_group_by_temp.style.format("{:.2f}")
        sheet_name = 'Группировка по ' + atach 
        df_group_by_temp.to_excel(writer, index = True, sheet_name=sheet_name)

