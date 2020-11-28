# Mimezone
Parse and recovery Nextcloude object storage

![Status](https://github.com/CrazyLionHeart/Mimezon/workflows/Python application/badge.svg)


### Long story short

Однажды я потерял базу данных Nextcloud, со всеми метаданными, хранимых объектов в Object Store поверх Amazon S3

"Говно-вопрос", подумал я. "Напишу рекавери программу"

Перебирает элементы в `source` S3-like storage, котором хранились (хранятся) данные Nextcloud, пытается по содержимому угадать content-type и переложить в `target` S3-like storage по пути: `recovery/X/Y`, где X - дата последней модификации исходного объекта, Y - ключ/имя объекта c предположительным расширеним.