#ifndef SYSTEM_TASKS_H
#define SYSTEM_TASKS_H

#include <Arduino_FreeRTOS.h>
#include <semphr.h>
#include <task.h>
#include "Device.h"

#define NUMBER_OF_TASKS 7

const ControlUnit centerUnit(PIN_COMPRESSOR, PIN_LAMP, PIN_FAN, PIN_IONIZER, PIN_BUZZER, true, KEY_READ, PIN_LM35, PIN_DOOR, sev, PIN_SEV0, PIN_SEV1);
extern TaskHandle_t tasks_handler[NUMBER_OF_TASKS];

const SemaphoreHandle_t serialSemaphore = xSemaphoreCreateMutex();
const SemaphoreHandle_t keySemaphore = xSemaphoreCreateMutex();
const SemaphoreHandle_t doorSemaphore = xSemaphoreCreateMutex();
const SemaphoreHandle_t temperatureSemaphore = xSemaphoreCreateMutex();
const SemaphoreHandle_t stateSemaphore = xSemaphoreCreateMutex();

void sensorTemperatureTask(void *pvParameters);

void sensorDoorTask(void *pvParameters);

void pullUpKeyTask(void *pvParameters);

void processTask(void *pvParameters);

void sevenSegmentUpdateTask(void *pvParameters);


boolean lock(QueueHandle_t semaphore, TickType_t timeout);
void unlock(QueueHandle_t semaphore);

const boolean logging = false;

#endif 