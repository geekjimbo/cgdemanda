#include <stdio.h>
#include "/usr/local/libxls/include/libxls/xls.h"

xlsWorkBook* pWB;
pWB=xls_open("RECIERRES20160529000011.XLS", "iso-8859-15//TRANSLIT");
xlsWorkSheet* pWS;
if (pWB!=NULL)
{
    printf("estamos vivos")
    xls_close_WB(pWB);            
}
else 
{
    printf("pWB == NULL\n");
}


