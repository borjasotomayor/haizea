BEGIN	{
    count = 0
    num = 0
}

NR == 1 {
    timeBegin = $1
}


$4 ~ beginEvent1 && event1 == "" {
    if (NR==1)
        timeBegin = $1
    time = $1 - timeBegin
    start1[$3]=time
}

$4 ~ beginEvent2 && event2 == "" {
    if (NR==1)
        timeBegin = $1
    time = $1 - timeBegin
    start2[$3]=time
}



function updateValues()
{
    num++
    time = $1 - timeBegin
    ratio = duration1[$3] / duration2[$3]
    count += ratio
    avg = count / num
    printf "%f %f %f\n", time, ratio, avg
}

$4 ~ endEvent1 && event1 == "" {
    time = $1 - timeBegin
    duration1[$3]=time - start1[$3]
    if ($3 in duration2)
	updateValues()
}

$4 ~ endEvent2 && event2 == "" {
    time = $1 - timeBegin
    duration2[$3]=time - start2[$3]
    if ($3 in duration1)
	updateValues()
}

beginEvent1 == "" && $4 ~ event1 {
    if (NR==1)
        timeBegin = $1
    time = $1 - timeBegin
    duration1[$3]=$5
    if ($3 in duration2)
	updateValues()
}

beginEvent2 == "" && $4 ~ event2 {
    if (NR==1)
        timeBegin = $1
    duration2[$3]=$5
    if ($3 in duration1)
	updateValues()
}
