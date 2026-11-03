#include <cpuid.h>
#include <stdint.h>
#include <stdio.h>
int main(void) {
 unsigned a,b,c,d;
 if (!__get_cpuid(1,&a,&b,&c,&d)) return 1;
 unsigned ecx=(1u<<0)|(1u<<9)|(1u<<12)|(1u<<13)|(1u<<19)|(1u<<20)|(1u<<22)|(1u<<23)|(1u<<26)|(1u<<27)|(1u<<28)|(1u<<29);
 unsigned edx=(1u<<15)|(1u<<23)|(1u<<24)|(1u<<25)|(1u<<26);
 if ((c&ecx)!=ecx || (d&edx)!=edx) {puts("Missing v1/v2/v3 CPUID1 or OSXSAVE");return 2;}
 unsigned lo,hi;__asm__ volatile("xgetbv":"=a"(lo),"=d"(hi):"c"(0));
 if ((lo&6)!=6) {puts("OS lacks XMM/YMM XCR0 state");return 3;}
 if (!__get_cpuid_count(7,0,&a,&b,&c,&d) || (b&((1u<<3)|(1u<<5)|(1u<<8)))!=((1u<<3)|(1u<<5)|(1u<<8))) return 4;
 if (!__get_cpuid(0x80000001,&a,&b,&c,&d) || (c&((1u<<0)|(1u<<5)))!=((1u<<0)|(1u<<5))) return 5;
 puts("CPUID x86-64-v3 plus OS XCR0 checks PASS");return 0;
}
