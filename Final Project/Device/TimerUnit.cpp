#include "TimerUnit.h"

TimerUnit::TimerUnit() {}

void TimerUnit::set(int index) 
{
    if(index > 3)
        return;
    timers[index] = millis();
}

unsigned long TimerUnit::getDelay(int index) 
{
    if(index > 3)
        return 0;
    return millis() - timers[index];
}
