# Results of Yandex tank

   ### 1.Лента с PUTами с уникальными ключами 
   Пограничное значение при значении 2400 запросов в секунду.
   Для константного режима было выбрано значение 1650 запросов в секунду.
  
   [Line](https://overload.yandex.net/230252#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574526257&slider_end=1574526620)

   [Const](https://overload.yandex.net/230298#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574528511&slider_end=1574528811)

   Технические характеристики компьютера не позволяют (точнее, обнаруживается значительное проседание) стрелять лентами с нагрузкой в 1650 запросов в секунду (на 30% ниже точки разладки)

   [Тест с 1450 запросами в секунду для константы](https://overload.yandex.net/230303#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574528943&slider_end=1574529243)

   В обоих случаях были выявлены просадки

   Дальнейшие эксперименты продолжаются при нагрузке на 40% ниже точки разладки

   ### 2. Лента с GETами существующих ключей с равномерным распределением (стреляем по наполненной БД)

   Пограничное значение при 2500 запросов в секунду
   [Line](https://overload.yandex.net/230276#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574527336&slider_end=1574527700)

   Для константного режима было выбрано значение 1500 запросов в секунду (на 40% меньше точки разладки)

   [Const](https://overload.yandex.net/230317#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574529942&slider_end=1574530242)

   ### 3. То же самое, но со смещением распределения GETов к недавно добавленным ключам (частый случай на практике)

   [Line](https://overload.yandex.net/230286#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574527930&slider_end=1574528291)

   Пограничное значение при 2800 запросов в секунду
   Для константного режима было выбрано значение 1700 запросов в секунду (на 40% меньше точки разладки)

   [Const](https://overload.yandex.net/230320#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574530455&slider_end=1574530755)

   ### 4. Лента с PUTами с частичной перезаписью ключей (вероятность 10%)
   Пограничное значение при 2500 запросов в секунду

   [Line](https://overload.yandex.net/230333#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574531227&slider_end=1574531592)

   Для константного режима было выбранозначение 1500 запросов в секунду (на 40% меньше точки разладки)

   [Const](https://overload.yandex.net/230342#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574531893&slider_end=1574532193)
   
   Практически на всем промежутке получено заданное число респонсов

   #### 5. лента со смешанной нагрузкой с 50% PUTы новых ключей и 50% GETы существующих ключей
   Среднее количество запросов put ~ 50.18%, среднее количсетво запросов get ~ 49.82%
   
   [Line](https://overload.yandex.net/230356#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574532594&slider_end=1574532947)

   Пограничное значение при 2800 запросов в секунду
   Для константного режима было выбрано значение 1700 запросов в секунду (на 40% меньше точки разладки)

   [Const](https://overload.yandex.net/230359#tab=test_data&tags=&plot_groups=main&machines=&metrics=&slider_start=1574533055&slider_end=1574533355)