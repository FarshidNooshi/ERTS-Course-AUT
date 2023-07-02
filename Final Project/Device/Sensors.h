#ifndef SENSOR_UNIT_H
#define SENSOR_UNIT_H
#include "TimerUnit.h"

#define NO_INPUT -1
#define POWER  0
#define MUTE   1
#define UP     2
#define DOWN   3
#define SUPER  4

class SensorUnit
{
      private:
            int pinLm35;
            int pinDoor;
            int pinKey;
            float currentTemperature;
            bool doorClose;

            // pre initialize
            unsigned long debounceDelay[6] = {50, 500, 50, 50, 50, 500};
            int buttonState = -1;
            int lastButtonState = -1;

            int pushbuttonsCount = 5;
            float sourceVoltage = 5.0f;
            float resitorValue = 1000.0f;
            float resitorToGround = 100000.0f;
            float V_tolerance = 0.1f;

            int calculateKey();
      protected:
            TimerUnit timer;
            void begin();
      public:
            SensorUnit();
            SensorUnit(int pinLm35, int pinDoor, int pinKey);
            float getCurrentTemperature();
            bool isDoorClose();
            int readKey();
            void updateTemperature();
            void updateDoor();
};

#endif
