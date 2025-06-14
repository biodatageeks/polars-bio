# Jak to działa?

Na przykładzie tej sekwencji (poniżej cała jedna sekwencja):
```` py

@SRR9130495.1 D00236:723:HG32CBCX2:1:1108:1330:1935/1
NCAATACAAAAGCAATATGGGAGAAGCTACCTACCATGCTTAAAAACGCCAATGAGCAGNGATTTGTCANCNNNNNNNNCNNNNNNNNTNNTANNANNCTC
+
#4BDFDFFHGHGGJJJHIIIIGGIIJGJJGIIIIBHIJJJIIJIJJIJDHIGGGIJJJI#-@AEHGEFF#,########,########+##++##+##+2<

````

## Zgodnie z linijkami kolejno

1. '@identyfikator sekwencji' małpa symbolizuje początek rekordu --> dalej jest ciąg znaków a xxx.1 to liczba porządkowa
2. sekwencja genetyczna DNA czyli zapisane geny gdzie N - niepewny odczyt
3. '+' separator "jakości" czyli po tym są wyniki
4. jakość odczytu (Czyli ten Phred Score) dla każdego nukleotydu (genu) w postaci znaków ASCII

W tabeli wynikowej jeden wiersz odpowiada jednej sekwencji 

