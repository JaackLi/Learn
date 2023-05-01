input="cooja.testlog"
while IFS= read -r line
do
  sleep 0.2
  echo "$line"
done < "$input"
