#include "Sensors.h"
#include "math.h"

SensorUnit::SensorUnit()
{
      timer = TimerUnit();
}

SensorUnit::SensorUnit(int pinLm35, int pinDoor, int pinKey)
{
    this->pinLm35 = pinLm35;
    this->pinDoor = pinDoor;
    this->pinKey = pinKey;
    timer = TimerUnit();
}

void SensorUnit::begin()
{
    pinMode(pinDoor, INPUT);
}

void SensorUnit::updateTemperature()
{
    currentTemperature = 40.0f * analogRead(pinLm35) / 325;
}

void SensorUnit::updateDoor()
{
    doorClose = digitalRead(pinDoor);
}

float SensorUnit::getCurrentTemperature()
{
    return currentTemperature;
}

bool SensorUnit::isDoorClose()
{
    return doorClose;
}

int SensorUnit::readKey()
{
    while(true)
    {
        int reading = calculateKey();
        
        if(reading != lastButtonState)
            timer.set(DEBOUNCE_TIMER);
        
        if (timer.getDelay(DEBOUNCE_TIMER) > debounceDelay[reading+1]) {
            if (reading != buttonState) {
                buttonState = reading;
                return buttonState;
            } else {
                return NO_INPUT;
            }
        }
        
        lastButtonState = reading;
    }
}

int SensorUnit::calculateKey()
{
    float VR_Calculated, RT, voltageMinimum, voltageMaximum, voltage = sourceVoltage * analogRead(pinKey) / 1023;

    for(unsigned int i=pushbuttonsCount; i > 0; i--)
    {
        RT = 1/(i * resitorValue) + 1/resitorToGround;
        RT = 1/RT;

        VR_Calculated =(sourceVoltage * RT) / ((pushbuttonsCount - i + 1) * resitorValue + RT);

        voltageMinimum = (1 - V_tolerance) * VR_Calculated;
        voltageMaximum = (1 + V_tolerance) * VR_Calculated;

        if(voltageMinimum < voltage && voltage < voltageMaximum)
        return pushbuttonsCount - i;
    }
      
    return NO_INPUT;
}
