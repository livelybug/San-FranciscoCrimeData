function genGraphLine(charId, xLabel, data) {
    
    data[0].shift();
    if(data[0][data[0].length-1] == "") data[0].pop();

    for(var i = 1; i < (data.length); i++) {
        if(data[i][data[i].length-1] == undefined) data[i].pop();
    }
        
    var sortings = data[0];
    data.shift()
    
    var chart = c3.generate({
        bindto: charId,

        data: {
            columns: data,
            type: 'line'
        },

        axis: {
            x: {
                label: {
                    text: xLabel,
                    position: 'outer-right'
                },
                type: 'category',
                categories: sortings
            },
            y: {
                label: 'Crime Number',
                min: 0,
                padding: {top:0, bottom:0}
            }
        }
    });
}

parseData('data\\byDistrictYear.csv', genGraphLine.bind(null, '#byDistrictYearChart', 'Year'))
