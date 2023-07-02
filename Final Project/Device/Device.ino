#include "SystemTasks.h"

void setup()
{
    Serial.begin(9600);
    Serial1.begin(9600);

    // Use a consistent indentation style and add comments to explain code
    xTaskCreate(
        sensorTemperatureTask,
        "Temperature Sensor Task",
        configMINIMAL_STACK_SIZE,
        NULL,
        1,
        &tasks_handler[0]
    );

    xTaskCreate(
        sensorDoorTask,
        "Door Sensor Task",
        configMINIMAL_STACK_SIZE,
        NULL,
        2,
        &tasks_handler[1]
    );

    xTaskCreate(
        pullUpKeyTask,
        "Pull Up Key Task",
        configMINIMAL_STACK_SIZE,
        NULL,
        3,
        &tasks_handler[2]
    );

     xTaskCreate(
        processTask,
        "Process Task",
        configMINIMAL_STACK_SIZE,
        NULL,
        3,
        &tasks_handler[3]
    );

    xTaskCreate(
        sevenSegmentUpdateTask,
        "Update Seven Segment Task",
        configMINIMAL_STACK_SIZE,
        NULL,
        4,
        &tasks_handler[4]
    );

    vTaskStartScheduler();
}

void loop(){}
