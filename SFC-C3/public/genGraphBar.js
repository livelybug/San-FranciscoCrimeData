
function parseData(fileName, genGraph) {
	Papa.parse(fileName, {
        download: true,
		complete: function(results) {
            
            var rotated = results.data[0].map(function(col, i) { 
              return results.data.map(function(row) { 
                return row[i] 
              })
            });
			genGraph(rotated);
		}
	});
}


function genGraphBar(graphName, charId, xLabel, data) {
    data[0].shift();
    if(data[0][data[0].length-1] == "") data[0].pop();
    if(data[1][data[1].length-1] == undefined) data[1].pop();
    data[1][0] = graphName;

    var sortings = data[0];
    var criNum = data[1];
        
    console.log(sortings)
    console.log(criNum)

    var chart = c3.generate({
        bindto: charId,

        data: {
            columns: [
                criNum
            ],
            type: 'bar'
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
        },
        
        bar: {
            width: {
                ratio: 0.5 // this makes bar width 50% of length between ticks
            }        
        }
    });
}

parseData('data\\byYear.csv', genGraphBar.bind(null, 'Crime Number by Year', '#byYearChart', 'Year'))
parseData('data\\byMonth.csv', genGraphBar.bind(null, 'Crime Number by Month', '#byMonthChart', 'Month'))
parseData('data\\byDayOfWeek.csv', genGraphBar.bind(null, 'Crime Number by Day of Week', '#byDayOfWeekChart', 'Day of Week'))
parseData('data\\byDistrict.csv', genGraphBar.bind(null, 'Crime Number of Top 10 Districts', '#byDistrict', 'District'))
