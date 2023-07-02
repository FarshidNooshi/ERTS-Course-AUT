#ifndef DEVICE_CODE_H
#define DEVICE_CODE_H

#include "Control.h"

/* 
 *  Sensors and Input interfaces Pins
*/
#define KEY_READ                A0
#define PIN_LM35                A1
#define PIN_DOOR                43

/* 
 *  Output Relays Pins
*/
#define PIN_COMPRESSOR          22
#define PIN_LAMP                23
#define PIN_FAN                 24
#define PIN_IONIZER             25
#define PIN_BUZZER              42

/* 
 *  Seven segment OutPut
*/
#define PIN_SEV0  44
#define PIN_SEV1  45
const int sev[] = {26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
#endif
