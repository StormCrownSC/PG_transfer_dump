# Используем официальный образ PostgreSQL как базовый
FROM postgres:14

# Устанавливаем необходимые утилиты
RUN apt-get update && \
    apt-get install -y postgresql-client

# Устанавливаем рабочую директорию
WORKDIR /usr/local/bin

# Устанавливаем Go (если нужен компилятор Go для выполнения скрипта)
RUN apt-get install -y golang

# Копируем скрипт в контейнер
COPY transfer_db.go /usr/local/bin/transfer_db.go

# Копируем и устанавливаем зависимости Go
RUN go mod init transfer_db && \
    go mod tidy && \
    go build -o transfer_db transfer_db.go

# Выполняем скрипт при запуске контейнера
ENTRYPOINT ["./transfer_db"]
