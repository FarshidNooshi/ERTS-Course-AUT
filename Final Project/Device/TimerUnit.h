#ifndef TIMER_UNIT_H
#define TIMER_UNIT_H
#include "Arduino.h"

#define NOT_STARTED             0

#define SEV_SEG_TIMER           0
#define STATE_TIMER             1
#define DOOR_TIMER              2
#define DEBOUNCE_TIMER          3


class TimerUnit
{
      private:
            /* 
                seven segment update timer
                state timer
                debouncing timer
            */
            unsigned long timers[4] = {NOT_STARTED, NOT_STARTED, NOT_STARTED, NOT_STARTED};
      public:
            TimerUnit();
            void set(int index);
            unsigned long getDelay(int index);

};
#endif
