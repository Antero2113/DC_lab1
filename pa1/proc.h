#ifndef __PA1_PROC__H
#define __PA1_PROC__H

#include "ipc.h"

#define MAX_IDS (MAX_PROCESS_ID + 1)

typedef struct {
    local_id id;
    int n;
    int rd[MAX_IDS];
    int wr[MAX_IDS];
    local_id last_from;
} Proc;

#endif
