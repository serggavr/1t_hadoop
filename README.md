#### Решение

Копируем в `.\tmp\` докер контейнера 1t_hadoop-datanode-1 папку с фалами `./for_load` с хоста
``` 
docker cp ./for_load 1t_hadoop-datanode-1:tmp\
```  
Подключаемся к контейнеру  
```
docker container exec -it 1t_hadoop-datanode-1 bash
```  
Переходим в папку `tmp\for_load`
```
cd tmp
cd for_load
```  
Схлопываем все txt файлы в папке в один sum.txt
```
cat *.txt >> sum.txt
```
Удаляем все файлы кроме sum.txt в папке `tmp\for_load`
```
find . -type f -not -name 'sum.txt' -delete
find . -type f -not -name 'sum.txt' -print0 | xargs -0 -I {} rm -v {}
```

Создаем папку в Hadoop
``` 
hadoop fs -mkdir /user/hive/my_loaded
```
Загружаем sum.txt на hdfs в папку `/user/hive/my_loaded`
``` 
hadoop fs -put /tmp/for_load/sum.txt /user/hive/my_loaded/sum.txt
```
Определяем права доступа к файлу `/user/hive/my_loaded/sum.txt`
``` 
hadoop fs -chmod 755  /user/hive/my_loaded/sum.txt
```
Смотрим информацию о папке `/user/hive/my_loaded/`
``` 
hadoop fs -ls /user/hive/my_loaded/
```
Смотрим информацию о размере файла `/user/hive/my_loaded/sum.txt`
``` 
hadoop fs -du /user/hive/my_loaded/sum.txt
```
Изменяем фактор репликации файла на 2 `/user/hive/my_loaded/sum.txt`
``` 
hadoop dfs -setrep 2  /user/hive/my_loaded/sum.txt
```
Смотрим информацию о размере файла `/user/hive/my_loaded/sum.txt`
``` 
hadoop fs -du /user/hive/my_loaded/sum.txt
```
Смотрим количество строк в файла `/user/hive/my_loaded/sum.txt`
``` 
hadoop fs -cat /user/hive/my_loaded/sum.txt | wc -l
```
