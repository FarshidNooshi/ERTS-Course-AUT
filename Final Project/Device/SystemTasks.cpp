#include "SystemTasks.h"

TaskHandle_t tasks_handler[NUMBER_OF_TASKS];
boolean holdState[3] = {false, false, false};

void vApplicationStackOverflowHook(TaskHandle_t xTask, char *pcTaskName)
{
    // Task has overflowed its stack
    // Release any semaphores it was holding
    Serial1.println("crached");
    if (xTask == tasks_handler[0])
    {
        xSemaphoreGive(serialSemaphore);
    }
    else if (xTask == tasks_handler[1])
    {
        xSemaphoreGive(serialSemaphore);
    }
}

void sensorTemperatureTask(void *pvParameters)
{
    TickType_t LastWakeTime = 0;
    TickType_t period = pdMS_TO_TICKS(5000); // 5 seconds
    TickType_t timeout = pdMS_TO_TICKS(5000); // 5 second timeout
    vTaskDelayUntil(&LastWakeTime, pdMS_TO_TICKS(300));
    while (true)
    {
        if (lock(temperatureSemaphore, timeout) == pdTRUE) {
            centerUnit.updateTemperature();
            unlock(temperatureSemaphore);
            xTaskNotifyGive(tasks_handler[3]);
        } else {
            continue;
        }

        if (logging && lock(serialSemaphore, timeout) == pdTRUE)
        {
            Serial.println("Temperature Sensor Task");
            unlock(serialSemaphore);
        } else if (logging) {
            continue;
        }
        vTaskDelayUntil(&LastWakeTime, period);
    }
}

void sensorDoorTask(void *pvParameters)
{
    TickType_t LastWakeTime = 0;
    TickType_t period = pdMS_TO_TICKS(1000); // 1 second
    TickType_t timeout = pdMS_TO_TICKS(1000); // 1 second timeout
    while (true)
    {
        if (lock(doorSemaphore, timeout) == pdTRUE) {
            centerUnit.updateDoor();
            unlock(doorSemaphore);
            xTaskNotifyGive(tasks_handler[3]);
        } else {
            continue;
        }

        if (logging && lock(serialSemaphore, timeout) == pdTRUE)
        {
            Serial.println("Door Sensor Task");
            unlock(serialSemaphore);
        } else if (logging) {
            continue;
        }
        vTaskDelayUntil(&LastWakeTime, period);
    }
}

void pullUpKeyTask(void *pvParameters)
{
    TickType_t LastWakeTime = 0;
    TickType_t period = pdMS_TO_TICKS(100); // 0.2 second
    TickType_t timeout = pdMS_TO_TICKS(100); // 0.2 second timeout
    while (true)
    {
        int key = NO_INPUT;
        if (lock(keySemaphore, timeout) == pdTRUE) {
            key = centerUnit.pullUpKey();
            unlock(keySemaphore);
            xTaskNotifyGive(tasks_handler[3]);
        } else {
            continue;
        }

        if (logging && lock(serialSemaphore, timeout) == pdTRUE)
        {
            Serial.println(key);
            unlock(serialSemaphore);
        } else if (logging) {
            continue;
        }
        vTaskDelayUntil(&LastWakeTime, period);
    }
}


void processTask(void *pvParameters)
{
    TickType_t timeout = pdMS_TO_TICKS(500); // 0.5 second timeout
    while (true)
    {
        int key = NO_INPUT;
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
        if (lock(keySemaphore, timeout) == pdTRUE) {
            key = centerUnit.getKey();
            unlock(keySemaphore);
        } else {
            continue;
        }

        centerUnit.process(key);
        if (lock(stateSemaphore, timeout) == pdTRUE) {
            for (int i = 0; i < 3; i++)
                holdState[i] = holdState[i] || centerUnit.getSevState()[i];
            unlock(stateSemaphore);
        } else {
            continue;
        }

        xTaskNotifyGive(tasks_handler[4]);

        if (logging && lock(serialSemaphore, timeout) == pdTRUE)
        {
            Serial.println("Process Task");
            unlock(serialSemaphore);
        } else if (logging) {
            continue;
        }
    }
}

void sevenSegmentUpdateTask(void *pvParameters) {
    TickType_t LastWakeTime = 0;
    TickType_t period = pdMS_TO_TICKS(250); // 0.2 second
    TickType_t timeout = pdMS_TO_TICKS(100); // 0.2 second timeout
    while (true)
    {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);

        int index;

        if (lock(stateSemaphore, timeout) == pdTRUE) {

            
            if (holdState[ERROR_DISPLAY]) {
                index = ERROR_DISPLAY;
            } else if (holdState[UPDATE_TEMP]) {
                index = UPDATE_TEMP;
            } else if (holdState[TEMP_DISPLAY]){
                index = TEMP_DISPLAY;
            }

            if (index != TEMP_DISPLAY)
                holdState[index] = false;

            unlock(stateSemaphore);
        } else {
            continue;
        }

        if (index != TEMP_DISPLAY) {
            centerUnit.sevOff();
        }

        centerUnit.updateSevSeg(index);

        if (index != TEMP_DISPLAY) {
            vTaskDelayUntil(&LastWakeTime, period);
            centerUnit.sevOn();
        }

        vTaskDelayUntil(&LastWakeTime, period * 2);
    }
}


boolean lock(QueueHandle_t semaphore, TickType_t timeout) {
    noInterrupts();
    boolean result = xSemaphoreTake(semaphore, timeout);
    interrupts();
    return result;
}

void unlock(QueueHandle_t semaphore) {
    noInterrupts();
    xSemaphoreGive(semaphore);
    interrupts();
}