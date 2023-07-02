#include "Control.h"

ControlUnit::ControlUnit(){}

ControlUnit::ControlUnit(int pinCompressor, int pinLamp, int pinFan, int pinIonizer, int pinBuzzer, bool isNormallyOpen, int pinKey, int pinLm35, int pinDoor, const int pinSev[], int pinSev0, int pinSev1)
: SensorUnit(pinLm35, pinDoor, pinKey), SettingUnit()
{
    compressor = Relay(pinCompressor, isNormallyOpen);
    lamp = Relay(pinLamp, isNormallyOpen);
    fan = Relay(pinFan, isNormallyOpen);
    ionizer = Relay(pinIonizer, isNormallyOpen);
    buzzer = Relay(pinBuzzer, isNormallyOpen);
    sev[1] = SegmentDisplay(pinSev1, pinSev[0], pinSev[1], pinSev[2], pinSev[3], pinSev[4], pinSev[5], pinSev[6], pinSev[7]);
    sev[0] = SegmentDisplay(pinSev0, pinSev[8], pinSev[9], pinSev[10], pinSev[11], pinSev[12], pinSev[13], pinSev[14], pinSev[15]);
}

void ControlUnit::updateSystem(int nextState)
{
    if(nextState < EMERGENCY || nextState > SUSPEND)
        return;

    Serial.println(getStatus(nextState));
    currentState = nextState;

    if(nextState == INIT) {
        compressor.begin();
        lamp.begin();
        fan.begin();
        ionizer.begin();
        buzzer.begin();
        sev[1].turnOn();
        sev[0].turnOn();
        return;
    }
        
    if(nextState == OFF) {
        compressor.turnOff();
        lamp.turnOff();
        fan.turnOff();
        ionizer.turnOff();
        buzzer.turnOff();

        sev[1].turnOff();
        sev[0].turnOff();
        return;
    }

    if(nextState == EMERGENCY) {
        compressor.turnOff();
        lamp.turnOff();
        fan.turnOff();
        ionizer.turnOff();
        buzzer.turnOn();
        return;
    }

    timer.set(STATE_TIMER);

    if(nextState == SUSPEND) {
        compressor.turnOff();
        lamp.turnOff();
        fan.turnOn();
        ionizer.turnOn();
        return;
    }

    if(nextState == COOLING) {
        compressor.turnOn();
        lamp.turnOff();
        fan.turnOn();
        ionizer.turnOn();
        buzzer.change(sound);
        return;
    }

    timer.set(DOOR_TIMER);

    if(nextState == DOOR_OPEN) {
        compressor.turnOff();
        lamp.turnOn();
        fan.turnOff();
        ionizer.turnOff();
        buzzer.change(sound);
        return;
    }

    if(nextState == FETAL_ERROR) {
        compressor.change(isDoorClose());
        lamp.change(!isDoorClose());
        fan.change(isDoorClose());
        ionizer.change(isDoorClose());
        buzzer.change(sound);
        return;
    }

}

int ControlUnit::getState()
{
    return currentState;
}

int ControlUnit::pullUpKey() {
    reading = readKey();
    return reading;
}

int ControlUnit::getKey() {
    return reading;
}

bool* ControlUnit::getSevState(){
    return state;
}

void ControlUnit::process(int key)
{     

    state[0] = false;
    state[1] = false;
    state[2] = true;

    if (key == POWER) {
        updateSystem(currentState == OFF); // true = 1 = INIT, false = 0 = OFF
        return;
    }

    if(currentState != OFF) 
        switch (key)
        {
        case MUTE:
            buzzer.turnOff();
            sound = false;
            break;
        
        case UP:
            incTemperature();
            state[UPDATE_TEMP] = true;
            break;
        
        case DOWN:
            decTemperature();
            state[UPDATE_TEMP] = true;
            break;

        case SUPER:
            SuperCoolingSwitch();
            break;
        }

    updateSystem(nextState());
    
}

int ControlUnit::nextState()
{
    switch (currentState)
    {

    case INIT:
        lastStateTemp = getCurrentTemperature();

        /* 
            conditon check is done
        */
        if(lastStateTemp - getTemperature() - tolerance > 0.000001f && isDoorClose()) 
            return COOLING;

        /* sensor init code */
        shouldRest = false;
        return SUSPEND;

    case COOLING:
        lastStateTemp = fmax(lastStateTemp, getCurrentTemperature());

        /* 
            conditon check is done
        */
        if(getCurrentTemperature() - warningTemp > 0.000001f  && timer.getDelay(STATE_TIMER) > WORKTIME) {
            shouldRest = false;
            sound = true;
            lastStateTemp = getCurrentTemperature();
            return FETAL_ERROR;
        }

        /*
            conditon check is done
        */
        if(!isDoorClose()) {
            shouldRest = false;
            sound = false;
            return DOOR_OPEN;
        }

        /*
                conditon check is done
        */
        if(getTemperature() >= getCurrentTemperature() || timer.getDelay(STATE_TIMER) > WORKTIME) {
            shouldRest = true;
            return SUSPEND;
        }

        break;

    case DOOR_OPEN:
        /*
            conditon check is done
        */
        if(getCurrentTemperature() - warningTemp > 0.000001f) {
            sound = true;
            return FETAL_ERROR;
        }

        /*
                conditon check is done
        */
        if(isDoorClose())
            return shouldRest ? SUSPEND : COOLING;
        
        /*
                conditon check is done
        */
        if(timer.getDelay(DOOR_TIMER) > OPENDOOR) {
            sound = true;
            return DOOR_OPEN;
        }
                
        break;

    case SUSPEND:
        /*
            conditon check is done
        */
        if(!isDoorClose()) 
            return DOOR_OPEN;

        /*
                conditon check is done
        */
        if(warningTemp <= getCurrentTemperature()) 
            return COOLING;

        /*
                conditon check is done
        */
        if(getCurrentTemperature() - getTemperature() - tolerance > 0.000001f && timer.getDelay(STATE_TIMER) > RESTTIME) 
            return COOLING;

        break;

    case FETAL_ERROR:
        state[ERROR_DISPLAY] = true;
        lastStateTemp = fmax(lastStateTemp, getCurrentTemperature());

        /*
                conditon check is done
        */
        if(!isDoorClose() && timer.getDelay(DOOR_TIMER) * 2 > OPENDOOR) {
            sound = true;
            lastStateTemp = getCurrentTemperature();
            return FETAL_ERROR;
        }

        /*
                conditon check is done
        */
        if(isDoorClose() ^ compressor.getState()) {
            sound = true;
            Serial.println(1);
            lastStateTemp = getCurrentTemperature();
            return FETAL_ERROR;
        }

        /*
                conditon check is done
        */
        if(getCurrentTemperature() < warningTemp) {
            sound = false;
            return isDoorClose() ? COOLING : DOOR_OPEN;
        }

        /*
                conditon check is done
        */
        if(timer.getDelay(STATE_TIMER) < LONGWORKTIME)
            return HOLDING;

        /*
                conditon check is done
        */
        if(getCurrentTemperature() - lastStateTemp < -5.0f) {
            sound = false;
            lastStateTemp = getCurrentTemperature();
            return FETAL_ERROR;
        }

        /*
                conditon check is done
        */
        if(timer.getDelay(STATE_TIMER) > LONGWORKTIME) {
            sound = true;
            return EMERGENCY;
        }

        break;

    case EMERGENCY:
        state[ERROR_DISPLAY] = true;
        break;

    }
    return HOLDING;
}

void ControlUnit::updateSevSeg(int index) {
    if(index == ERROR_DISPLAY) {
            switch (currentState) {
                case FETAL_ERROR:
                    sev[1].displayHex(15, false);
                    sev[0].displayHex(14, false); 
                    break;
                case EMERGENCY:
                    sev[1].displayHex(14, false);
                    sev[0].displayHex(14, false); 
                    break;
            }
    } else if(index == UPDATE_TEMP) {
        displayTemp(getTemperature());
    } else if(index == TEMP_DISPLAY) {
        displayTemp(getCurrentTemperature());
    }
}

void ControlUnit::sevOn() {
    if (currentState != OFF) {
        sev[1].turnOn();
        sev[0].turnOn();
    }
}

void ControlUnit::sevOff() {
    sev[1].turnOff();
    sev[0].turnOff();
}


void ControlUnit::displayTemp(float temp)
{
    if(temp < -9 || temp > 99)
        return;

    if(fabs(temp) < 1) {
        sev[1].displayHex(temp < 0 ? 16 : 0, true);
        sev[0].displayHex(round(fabs(temp) * 10), false);
    } else if(temp < 0) {
        sev[1].displayHex(16, false);
        sev[0].displayHex(round(fabs(temp)), false);
    } else if (temp < 10) {
        sev[1].displayHex(temp, true);
        sev[0].displayHex(floor((temp - floor(temp)) * 10), false);
    } else {
        sev[1].displayHex(round(temp) / 10, false);
        sev[0].displayHex(round(temp) % 10, false);
    }
}

String ControlUnit::getStatus(int currentStatus) {
    switch (currentStatus) {
        case -2:
            return "Emergency";
        case -1:
            return "Fetal Error";
        case 0:
            return "Off";
        case 1:
            return "Init Up";
        case 2:
            return "Cooling";
        case 3:
            return "Door is Open";
        case 4:
            return "Suspend";
        default:
            return "Not found";
    }
}
