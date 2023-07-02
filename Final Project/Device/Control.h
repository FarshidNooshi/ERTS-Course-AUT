#ifndef CONTROL_UNIT_H
#define CONTROL_UNIT_H
#include <SegmentDisplay.h>
#include <Relay.h>
#include "Sensors.h"
#include "Setting.h"


#define HOLDING        -3

#define EMERGENCY      -2
#define FETAL_ERROR    -1
#define OFF             0
#define INIT            1
#define COOLING         2
#define DOOR_OPEN       3
#define SUSPEND         4

#define UPDATE_TEMP     0
#define ERROR_DISPLAY   1
#define TEMP_DISPLAY    2

class ControlUnit : public SensorUnit, public SettingUnit
{
      private:
            int currentState = OFF;
            Relay compressor;
            Relay lamp;
            Relay fan;
            Relay ionizer;
            Relay buzzer;

            bool state[3] = {false, false, true};

            float lastStateTemp = 0.0f;

            bool show = true;

            bool shouldRest = false;
            bool sound = false;
            int reading = -1;

            void updateSystem(int nextState);
            int nextState();
            void enQueue(int value);
            int deQueue();
            SegmentDisplay sev[2];
      public:
            ControlUnit();
            ControlUnit(int pinCompressor, int pinLamp, int pinFan, int pinIonizer, int pinBuzzer, bool isNormallyOpen, int pinKey, int pinLm35, int pinDoor, const int pinSev[], int pinSev0, int pinSev1);
            int pullUpKey();
            int getState();
            int getKey();
            bool* getSevState();
            void process(int key);
            void updateSevSeg(int index);
            void displayTemp(float temp);
            String getStatus(int currentStatus);
            void ControlUnit::sevOn();
            void ControlUnit::sevOff();
};
#endif
