#include "Setting.h"
SettingUnit::SettingUnit(){}

void SettingUnit::save(){}

void SettingUnit::setTemperature(float temperature)
{
    this->temperature = temperature;
    save();
}

void SettingUnit::incTemperature()
{
    if(temperature < 14.0f)
        temperature++;
}

void SettingUnit::decTemperature()
{
    if(temperature > 4.0f)
        temperature--;
}

void SettingUnit::SuperCoolingSwitch()
{
    superCooling = !superCooling;
    Serial.println("Super Cooling: " + String(superCooling));
    save();
}

void SettingUnit::load(){}

float SettingUnit::getTemperature()
{
    return fmin(fmax(4.0f, superCooling ?  temperature - 3.0f : temperature), 14.0f);
}

bool SettingUnit::isSuperCooling()
{
    return superCooling;
}
