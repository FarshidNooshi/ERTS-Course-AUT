#ifndef SETTING_UNIT_H
#define SETTING_UNIT_H
#include "Arduino.h"

#define LONGWORKTIME 15000
#define WORKTIME 10000 // 30 min
#define RESTTIME 10000 // 10 min
#define OPENDOOR 10000 // 30 sec
const unsigned int segTime[] =  {2000, 1000};

class SettingUnit
{
      private:
            float temperature = 8.0f;
            bool superCooling = false;
            void save();
      protected:
            float tolerance = 3.0f;
            float warningTemp = 20.0f;
            void setTemperature(float temperature);
            void incTemperature();
            void decTemperature();
            void SuperCoolingSwitch();
            void load();
      public:
            SettingUnit();
            float getTemperature();
            bool isSuperCooling();
};

#endif
