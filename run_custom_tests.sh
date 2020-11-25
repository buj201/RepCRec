#!/bin/sh


echo "---------------------------------------------------------------------"
echo -e "Custom test 1: All sites fail\n"
echo -e "Description:"
echo -e "------------\n"
echo -e "This test involves all sites failing."
echo -e "Then, while all sites are down, a transaction requests to write."
echo -e "This transaction has to wait.\n"
echo -e "RepCRec instructions:"
echo -e "---------------------\n"
cat tests/custom_tests/allfail.txt
echo -e "\n\nRepCRec output:"
echo -e "-------------------\n"
python3 -m src.transaction_manager tests/custom_tests/allfail.txt
echo -e ""
echo "---------------------------------------------------------------------"
echo -e "\n\n\n"






echo "---------------------------------------------------------------------"
echo -e "Custom test 2: Read Only Transaction started while all sites down\n"
echo -e "Description:"
echo -e "------------\n"
echo -e "This test involves a read only transaction starting while all"
echo -e "sites are down. In my implementation, in this case the RO transaction."
echo -e "has to wait until all sites recover to ensure it has read the most"
echo -e "recent commit to the requested variable, where the commit occured."
echo -e "before the read only transaction began.\n"
echo -e "RepCRec instructions:"
echo -e "---------------------\n"
cat tests/custom_tests/RO_replicated_dead_sites.txt
echo -e "\n\nRepCRec output:"
echo -e "-------------------\n"
python3 -m src.transaction_manager tests/custom_tests/RO_replicated_dead_sites.txt
echo -e ""
echo "---------------------------------------------------------------------"
echo -e "\n\n\n"







echo "---------------------------------------------------------------------"
echo -e "Custom test 3: Writing after recovery"
echo -e "Description:"
echo -e "------------\n"
echo -e "This test illustrates writing after recovery. Specifically, writes"
echo -e "should write to all live sites with the target variable. Thus, if"
echo -e "a site comes back on line while a transaction is waiting to obtain"
echo -e "locks for a write, then it should also request locks at the newly"
echo -e "live site. This test demonstrates this behavior.\n"
echo -e "RepCRec instructions:"
echo -e "---------------------\n"
cat tests/custom_tests/write_recovered_site.txt
echo -e "\n\nRepCRec output:"
echo -e "-------------------\n"
python3 -m src.transaction_manager tests/custom_tests/write_recovered_site.txt
echo -e ""
echo "---------------------------------------------------------------------"

