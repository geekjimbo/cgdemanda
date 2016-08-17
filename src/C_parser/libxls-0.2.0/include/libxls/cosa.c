#include <stdio.h>
#include "xls.h"
//#include "/usr/local/libxls/lib/libxlsreader.a"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <time.h>
//
//#include <libxls/xls.h>
//
//#include "xlsformula.h"
//xlsWorkBook* pWB;
int main() 
{
    xlsWorkSheet* pWB;
    pWB=xls_open("RECIERRES20160529000011.XLS", "UTF-8");
    if (pWB != NULL) {
        printf("estamos vivos");
        xls_close_WB(pWB);            
    } 
    else {
        printf("pWB == NULL\n");
    }

}

