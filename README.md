    Запуск

    main.exe pass E:\P\1.txt

    строки из файла E:\P\1.txt будут добавлены в множество pass


    Получить данные из множества pass

    все данные

    SMEMBERS pass

    получить допустим 100 строк

    SSCAN pass 0 COUNT 100


    ///

    в коде есть параметр

    	const batchSize = 1000000