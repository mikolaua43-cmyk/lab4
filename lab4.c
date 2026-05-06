#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* Константы для работы программы */
enum {
    QUEUE_CAPACITY = 8,    // Размер очереди сообщений
    MAX_TEXT_SIZE  = 256,  // Максимальный размер данных в сообщении
    MAX_CHILDREN   = 64,   // Максимальное число производителей/потребителей
    WORK_DELAY_MS  = 250,  // Задержка между операциями (мс)
    OPS_PER_CHILD  = 8     // Количество операций, выполняемых каждым процессом
};

/* Идентификаторы семафоров */
enum {
    SEM_MUTEX = 0,  // Мьютекс для защиты критической секции
    SEM_EMPTY = 1,  // Семафор количества свободных мест в очереди
    SEM_FULL  = 2,  // Семафор количества заполненных мест в очереди
    SEM_COUNT = 3   // Всего семафоров
};

/* union для работы с semctl */
union semun {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
};

/* Структура одного сообщения */
struct message_s {
    uint8_t type;               // Тип сообщения
    uint16_t hash;              // Контрольная сумма сообщения
    uint8_t size;               // Реальный размер данных - 1
    unsigned char data[MAX_TEXT_SIZE]; // Данные сообщения
};

/* Общая структура состояния для всех процессов */
struct shared_state_s {
    size_t head;                // Индекс начала очереди
    size_t tail;                // Индекс конца очереди
    size_t count;               // Количество сообщений в очереди
    size_t free_space;          // Количество свободных мест

    unsigned long long produced_total; // Всего произведено сообщений
    unsigned long long consumed_total; // Всего потреблено сообщений

    size_t producers_alive;     // Количество живых производителей
    size_t consumers_alive;     // Количество живых потребителей

    struct message_s queue[QUEUE_CAPACITY]; // Кольцевая очередь сообщений
};

/* Глобальные переменные для IPC */
static int g_shmid = -1;                // Идентификатор сегмента памяти
static int g_semid = -1;                // Идентификатор семафоров
static struct shared_state_s *g_state = NULL; // Указатель на общую память

/* Массивы PID дочерних процессов */
static pid_t g_producers[MAX_CHILDREN];
static size_t g_producers_count = 0;

static pid_t g_consumers[MAX_CHILDREN];
static size_t g_consumers_count = 0;

/* Флаг завершения программы */
static volatile sig_atomic_t g_stop = 0;

/* Обработчик сигнала SIGTERM для корректного завершения */
static void
handle_sigterm(int signo)
{
    (void)signo;
    g_stop = 1;
}

/* Вычисление хеша сообщения для проверки целостности */
static uint16_t
compute_hash(const struct message_s *msg)
{
    uint32_t hash = 0u;
    uint32_t real_size = (uint32_t)msg->size + 1u;

    // Простая функция хеширования
    hash = (hash * 131u) + msg->type;
    hash = (hash * 131u) + 0u;
    hash = (hash * 131u) + 0u;
    hash = (hash * 131u) + msg->size;

    for (uint32_t i = 0; i < real_size; ++i) {
        hash = (hash * 131u) + msg->data[i];
    }

    return (uint16_t)(hash & 0xFFFFu);
}

/* Заполнение сообщения случайными данными */
static void
fill_message(struct message_s *msg, unsigned int *seed)
{
    uint32_t real_size = 1u + (uint32_t)(rand_r(seed) % 256u);

    msg->type = (uint8_t)(rand_r(seed) % 256u);
    msg->size = (uint8_t)(real_size - 1u);

    for (uint32_t i = 0; i < real_size; ++i) {
        msg->data[i] = (unsigned char)(rand_r(seed) % 256u);
    }

    // Дополняем нулями до MAX_TEXT_SIZE
    if (real_size < MAX_TEXT_SIZE) {
        memset(msg->data + real_size, 0, MAX_TEXT_SIZE - real_size);
    }

    msg->hash = compute_hash(msg);
}

/* Универсальная функция работы с семафорами */
static int
sem_do(unsigned short sem_num, short sem_op)
{
    struct sembuf op;

    op.sem_num = sem_num;
    op.sem_op = sem_op;
    op.sem_flg = 0;

    while (semop(g_semid, &op, 1) == -1) {
        if (errno == EINTR) {
            if (g_stop) {
                return -1; // Завершение при сигнале
            }
            continue;
        }
        perror("semop");
        return -1;
    }

    return 0;
}

/* Обертки для семафоров для удобства */
static int lock_mutex(void)   { return sem_do(SEM_MUTEX, -1); }
static int unlock_mutex(void) { return sem_do(SEM_MUTEX, +1); }
static int wait_empty(void)   { return sem_do(SEM_EMPTY, -1); }
static int post_empty(void)   { return sem_do(SEM_EMPTY, +1); }
static int wait_full(void)    { return sem_do(SEM_FULL, -1); }
static int post_full(void)    { return sem_do(SEM_FULL, +1); }

/* Короткий сон в миллисекундах */
static void
short_sleep(long milliseconds)
{
    struct timespec ts;

    ts.tv_sec = milliseconds / 1000;
    ts.tv_nsec = (milliseconds % 1000) * 1000000L;

    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
        if (g_stop) {
            break;
        }
    }
}

/* Главный цикл производителя */
static void
producer_loop(void)
{
    unsigned int seed = (unsigned int)(time(NULL) ^ (unsigned int)getpid());

    for (int iter = 0; iter < OPS_PER_CHILD && !g_stop; ++iter) {
        struct message_s msg;
        unsigned long long produced_total;
        size_t count_after;

        fill_message(&msg, &seed);

        // Ждем место в очереди
        if (wait_empty() == -1) {
            break;
        }

        // Защищаем критическую секцию
        if (lock_mutex() == -1) {
            (void)post_empty();
            break;
        }

        // Добавляем сообщение в очередь
        g_state->queue[g_state->tail] = msg;
        g_state->tail = (g_state->tail + 1u) % QUEUE_CAPACITY;
        g_state->count++;
        g_state->free_space--;
        g_state->produced_total++;

        produced_total = g_state->produced_total;
        count_after = g_state->count;

        (void)unlock_mutex();

        // Сообщаем, что появилось новое сообщение
        if (post_full() == -1) {
            break;
        }

        printf("producer pid=%ld: добавлено сообщение, всего добавлено=%llu, в очереди=%zu\n",
               (long)getpid(), produced_total, count_after);
        fflush(stdout);

        short_sleep(WORK_DELAY_MS);
    }

    _exit(EXIT_SUCCESS);
}

/* Главный цикл потребителя */
static void
consumer_loop(void)
{
    for (int iter = 0; iter < OPS_PER_CHILD && !g_stop; ++iter) {
        struct message_s msg;
        uint16_t expected_hash;
        unsigned long long consumed_total;
        size_t count_after;

        // Ждем сообщение в очереди
        if (wait_full() == -1) {
            break;
        }

        if (lock_mutex() == -1) {
            (void)post_full();
            break;
        }

        // Извлекаем сообщение
        msg = g_state->queue[g_state->head];
        g_state->head = (g_state->head + 1u) % QUEUE_CAPACITY;
        g_state->count--;
        g_state->free_space++;
        g_state->consumed_total++;

        consumed_total = g_state->consumed_total;
        count_after = g_state->count;

        (void)unlock_mutex();

        // Сообщаем, что освободилось место
        if (post_empty() == -1) {
            break;
        }

        // Проверка целостности сообщения
        expected_hash = compute_hash(&msg);

        printf("consumer pid=%ld: извлечено сообщение, hash=%s, всего извлечено=%llu, в очереди=%zu\n",
               (long)getpid(),
               (expected_hash == msg.hash) ? "ok" : "bad",
               consumed_total,
               count_after);
        fflush(stdout);

        short_sleep(WORK_DELAY_MS);
    }

    _exit(EXIT_SUCCESS);
}

/* Удаляем PID из массива (при завершении процесса) */
static void
remove_pid_from_array(pid_t *arr, size_t *count, pid_t pid)
{
    for (size_t i = 0; i < *count; ++i) {
        if (arr[i] == pid) {
            for (size_t j = i + 1; j < *count; ++j) {
                arr[j - 1] = arr[j];
            }
            (*count)--;
            return;
        }
    }
}

/* Проверяем и собираем завершившиеся дочерние процессы */
static void
reap_finished_children(void)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        bool was_producer = false;
        bool was_consumer = false;

        // Определяем, был ли это производитель
        for (size_t i = 0; i < g_producers_count; ++i) {
            if (g_producers[i] == pid) {
                was_producer = true;
                break;
            }
        }

        // Определяем, был ли это потребитель
        for (size_t i = 0; i < g_consumers_count; ++i) {
            if (g_consumers[i] == pid) {
                was_consumer = true;
                break;
            }
        }

        // Уменьшаем счетчики живых процессов
        if (was_producer) {
            remove_pid_from_array(g_producers, &g_producers_count, pid);
            if (lock_mutex() == 0) {
                if (g_state->producers_alive > 0u) {
                    g_state->producers_alive--;
                }
                (void)unlock_mutex();
            }
        }

        if (was_consumer) {
            remove_pid_from_array(g_consumers, &g_consumers_count, pid);
            if (lock_mutex() == 0) {
                if (g_state->consumers_alive > 0u) {
                    g_state->consumers_alive--;
                }
                (void)unlock_mutex();
            }
        }
    }
}

/* Создание нового производителя */
static int
spawn_producer(void)
{
    pid_t pid;

    reap_finished_children();

    if (g_producers_count >= MAX_CHILDREN) {
        fprintf(stderr, "Достигнут лимит производителей\n");
        return -1;
    }

    pid = fork();
    if (pid == -1) {
        perror("fork");
        return -1;
    }

    if (pid == 0) {
        producer_loop();
    }

    g_producers[g_producers_count++] = pid;

    if (lock_mutex() == 0) {
        g_state->producers_alive++;
        (void)unlock_mutex();
    }

    printf("parent pid=%ld: создан производитель pid=%ld\n",
           (long)getpid(), (long)pid);
    fflush(stdout);

    return 0;
}

/* Создание нового потребителя */
static int
spawn_consumer(void)
{
    pid_t pid;

    reap_finished_children();

    if (g_consumers_count >= MAX_CHILDREN) {
        fprintf(stderr, "Достигнут лимит потребителей\n");
        return -1;
    }

    pid = fork();
    if (pid == -1) {
        perror("fork");
        return -1;
    }

    if (pid == 0) {
        consumer_loop();
    }

    g_consumers[g_consumers_count++] = pid;

    if (lock_mutex() == 0) {
        g_state->consumers_alive++;
        (void)unlock_mutex();
    }

    printf("parent pid=%ld: создан потребитель pid=%ld\n",
           (long)getpid(), (long)pid);
    fflush(stdout);

    return 0;
}

/* Завершаем последнего производителя */
static void
stop_last_producer(void)
{
    pid_t pid;

    reap_finished_children();

    if (g_producers_count == 0u) {
        puts("Нет производителей для удаления");
        return;
    }

    pid = g_producers[g_producers_count - 1u];

    if (kill(pid, SIGTERM) == -1 && errno != ESRCH) {
        perror("kill");
        return;
    }

    (void)waitpid(pid, NULL, 0);
    remove_pid_from_array(g_producers, &g_producers_count, pid);

    if (lock_mutex() == 0) {
        if (g_state->producers_alive > 0u) {
            g_state->producers_alive--;
        }
        printf("parent pid=%ld: завершен производитель pid=%ld, осталось=%zu\n",
               (long)getpid(), (long)pid, g_state->producers_alive);
        fflush(stdout);
        (void)unlock_mutex();
    }
}

/* Завершаем последнего потребителя */
static void
stop_last_consumer(void)
{
    pid_t pid;

    reap_finished_children();

    if (g_consumers_count == 0u) {
        puts("Нет потребителей для удаления");
        return;
    }

    pid = g_consumers[g_consumers_count - 1u];

    if (kill(pid, SIGTERM) == -1 && errno != ESRCH) {
        perror("kill");
        return;
    }

    (void)waitpid(pid, NULL, 0);
    remove_pid_from_array(g_consumers, &g_consumers_count, pid);

    if (lock_mutex() == 0) {
        if (g_state->consumers_alive > 0u) {
            g_state->consumers_alive--;
        }
        printf("parent pid=%ld: завершен потребитель pid=%ld, осталось=%zu\n",
               (long)getpid(), (long)pid, g_state->consumers_alive);
        fflush(stdout);
        (void)unlock_mutex();
    }
}

/* Вывод состояния очереди и счетчиков */
static void
print_state(void)
{
    reap_finished_children();

    if (lock_mutex() == -1) {
        return;
    }

    printf("state: размер=%d, занято=%zu, свободно=%zu, добавлено=%llu, извлечено=%llu, производителей=%zu, потребителей=%zu\n",
           QUEUE_CAPACITY,
           g_state->count,
           g_state->free_space,
           g_state->produced_total,
           g_state->consumed_total,
           g_state->producers_alive,
           g_state->consumers_alive);
    fflush(stdout);

    (void)unlock_mutex();
}

/* Инициализация общей памяти и семафоров */
static int
init_ipc(void)
{
    union semun arg;

    g_shmid = shmget(IPC_PRIVATE, sizeof(struct shared_state_s), IPC_CREAT | 0600);
    if (g_shmid == -1) {
        perror("shmget");
        return -1;
    }

    g_state = (struct shared_state_s *)shmat(g_shmid, NULL, 0);
    if (g_state == (void *)-1) {
        perror("shmat");
        g_state = NULL;
        return -1;
    }

    memset(g_state, 0, sizeof(*g_state));
    g_state->free_space = QUEUE_CAPACITY;

    g_semid = semget(IPC_PRIVATE, SEM_COUNT, IPC_CREAT | 0600);
    if (g_semid == -1) {
        perror("semget");
        return -1;
    }

    // Инициализация семафоров
    arg.val = 1;
    if (semctl(g_semid, SEM_MUTEX, SETVAL, arg) == -1) {
        perror("semctl mutex");
        return -1;
    }

    arg.val = QUEUE_CAPACITY;
    if (semctl(g_semid, SEM_EMPTY, SETVAL, arg) == -1) {
        perror("semctl empty");
        return -1;
    }

    arg.val = 0;
    if (semctl(g_semid, SEM_FULL, SETVAL, arg) == -1) {
        perror("semctl full");
        return -1;
    }

    return 0;
}

/* Очистка IPC ресурсов */
static void
cleanup_ipc(void)
{
    if (g_state != NULL) {
        (void)shmdt(g_state);
        g_state = NULL;
    }

    if (g_shmid != -1) {
        (void)shmctl(g_shmid, IPC_RMID, NULL);
        g_shmid = -1;
    }

    if (g_semid != -1) {
        (void)semctl(g_semid, 0, IPC_RMID);
        g_semid = -1;
    }
}

/* Завершение всех производителей */
static void
stop_all_producers(void)
{
    reap_finished_children();
    while (g_producers_count > 0u) {
        stop_last_producer();
        reap_finished_children();
    }
}

/* Завершение всех потребителей */
static void
stop_all_consumers(void)
{
    reap_finished_children();
    while (g_consumers_count > 0u) {
        stop_last_consumer();
        reap_finished_children();
    }
}

/

