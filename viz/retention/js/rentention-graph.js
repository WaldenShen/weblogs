/**
 * Created by Araja Jyothi Babu on 22-May-16.
 */
/*
(function(factory) {
    'use strict';
    if (typeof define === 'function' && define.amd) {
        define(['jquery'], factory);
    } else if (typeof exports !== 'undefined') {
        module.exports = factory(require('jquery'));
    } else {
        factory(jQuery);
    }

}(function($) {
    'use strict';
    var Retention = window.Retention || {};

    Retention = (function() {

        function Retention(element, options) {

        }
        return Retention;

    }());

    $.fn.retention = function() {

    };

}));*/



function getRows(data){
    var rows = [];
    var keys = Object.keys(data);
    var days = [];
    var percentDays = [];
    for(var key in keys){
        if(data.hasOwnProperty(keys[key])) {
            days = data[keys[key]];
            percentDays.push(keys[key]);
            for(var i = 0; i < days.length; i++){
                percentDays.push(i > 0 ? Math.round((days[i]/days[0] * 100) * 100) / 100 : days[i]);
            }
            rows.push(percentDays);
            percentDays = [];
        }
    }
    return rows;
}

function isValidHex(color){
    return /(^#[0-9A-F]{6}$)|(^#[0-9A-F]{3}$)/i.test(color);
}

function shadeColor(color, percent) { //#
    color = isValidHex(color) ? color : "#3f83a3"; //handling null color;
    percent = 1.0 - Math.ceil(percent / 10) / 10;
    var f=parseInt(color.slice(1),16),t=percent<0?0:255,p=percent<0?percent*-1:percent,R=f>>16,G=f>>8&0x00FF,B=f&0x0000FF;
    return "#"+(0x1000000+(Math.round((t-R)*p)+R)*0x10000+(Math.round((t-G)*p)+G)*0x100+(Math.round((t-B)*p)+B)).toString(16).slice(1);
}

function tooltipData(count, dayIndex){
    return  (count + "% of users were active after " + (dayIndex - 1)) + (dayIndex == 2 ? "day" : " days");
}

var options = {
        data: {
'2016-08-06': [31790,842,876,619,461,363,276,211,1146,783,551,1460,24202],
'2016-08-07': [28217,1047,898,467,373,295,191,158,1104,689,535,1309,21151],
'2016-08-08': [56772,2562,2296,1172,817,400,161,451,2217,1330,1120,2368,41878],
'2016-08-09': [55386,2418,2089,1124,506,188,469,619,2197,1248,1067,2135,41326],
'2016-08-10': [58987,2559,2289,822,243,609,799,672,2314,1489,1147,2204,43840],
'2016-08-11': [59911,2518,1743,376,871,977,720,594,2339,1463,1139,1991,45180],
'2016-08-12': [56684,983,556,1458,1443,869,654,536,2125,1315,1013,1787,43945],
'2016-08-13': [13143,94,362,291,182,134,94,109,497,331,240,414,10395],
'2016-08-14': [35297,1283,1110,604,409,322,331,267,1299,768,645,956,27303],
'2016-08-15': [62667,2694,2519,1228,816,533,319,443,2324,1481,1207,1742,47361],
'2016-08-16': [62516,2509,2247,1204,678,357,497,619,2297,1519,1139,1617,47833],
'2016-08-17': [60343,2313,2122,842,446,564,677,556,2054,1391,961,1492,46925],
'2016-08-18': [57620,2269,1659,542,702,834,580,486,1989,1274,766,1319,45200],
'2016-08-19': [54641,1313,974,1357,1215,768,581,483,1978,1169,691,1359,42753],
'2016-08-20': [39910,1011,971,637,441,339,283,339,1499,1287,656,823,31624],
'2016-08-21': [31987,1097,905,482,349,293,274,241,1139,762,467,711,25267],
'2016-08-22': [59299,2479,2270,1127,839,430,281,432,2166,1386,654,1323,45912],
'2016-08-23': [59088,2212,1969,1056,572,331,438,583,2014,1222,737,1150,46804],
'2016-08-24': [56863,2214,1945,794,378,491,622,471,2086,1150,695,1041,44976],
'2016-08-25': [55138,2143,1466,477,693,799,628,480,2017,1004,760,869,43802],
'2016-08-26': [51421,1183,870,1138,1139,707,544,442,1752,858,744,647,41397],
'2016-08-27': [32094,940,835,589,406,312,236,239,1091,544,465,408,26029],
'2016-08-28': [31196,1003,910,519,359,274,216,200,1118,529,462,331,25275],
'2016-08-29': [55175,2369,2116,1073,726,413,212,399,1970,758,947,452,43740],
'2016-08-30': [58869,2281,2004,962,489,250,454,593,1914,846,893,392,47791],
'2016-08-31': [59754,2229,1875,768,379,618,688,561,1968,961,743,431,48533],
'2016-09-01': [54300,2183,1552,478,791,885,655,537,1761,1090,760,271,43337],
'2016-09-02': [50666,1179,886,1322,1247,750,536,446,1559,1137,738,71,40795],
'2016-09-03': [29897,879,848,630,356,345,226,206,856,602,389,0,24560],
'2016-09-04': [28521,1111,901,429,326,252,255,209,812,608,347,0,23271],
'2016-09-05': [64269,3025,2557,1231,887,605,363,327,1717,1597,710,0,51250],
'2016-09-06': [58756,2581,2202,1006,680,438,380,383,1622,1263,556,0,47645],
'2016-09-07': [60355,2342,2002,957,587,451,499,482,1675,1078,397,0,49885],
'2016-09-08': [53201,2217,1771,610,478,571,446,284,1639,968,255,0,43962],
'2016-09-09': [48314,1887,1227,757,638,518,317,183,1707,869,83,0,40128],
'2016-09-10': [46787,1180,1297,908,654,353,209,175,1743,842,0,0,39426],
'2016-09-11': [28264,925,797,467,291,209,189,163,1105,516,0,0,23602],
'2016-09-12': [44017,1567,1484,634,272,220,207,410,1817,579,0,0,36827],
'2016-09-13': [40671,1845,1095,385,269,233,480,583,1423,492,0,0,33866],
'2016-09-14': [43485,981,807,444,328,781,868,514,1410,417,0,0,36935],
'2016-09-15': [24876,815,652,399,367,342,269,181,779,159,0,0,20913],
'2016-09-16': [23373,718,528,428,403,245,168,152,686,37,0,0,20008],
'2016-09-17': [22717,729,665,457,273,225,180,147,689,0,0,0,19352],
'2016-09-18': [25653,981,832,471,281,248,188,143,669,0,0,0,21840],
'2016-09-19': [57629,2914,2401,1132,733,444,227,393,1350,0,0,0,48035],
'2016-09-20': [60104,2453,2071,1134,625,275,441,450,1210,0,0,0,51445],
'2016-09-21': [53397,2258,1854,728,318,467,465,252,1004,0,0,0,46051],
'2016-09-22': [49389,2049,1471,433,603,547,233,397,718,0,0,0,42938],
'2016-09-23': [45805,1161,826,1152,819,263,443,553,237,0,0,0,40351],
'2016-09-24': [26239,762,794,480,296,244,247,115,0,0,0,0,23301],
'2016-09-25': [25178,927,797,372,312,244,117,0,0,0,0,0,22409],
'2016-09-26': [48727,1370,984,1290,1253,376,0,0,0,0,0,0,43454],
'2016-09-27': [28911,802,822,612,214,0,0,0,0,0,0,0,26461],
'2016-09-28': [28783,1069,922,263,0,0,0,0,0,0,0,0,26529],
'2016-09-29': [51796,2484,1305,0,0,0,0,0,0,0,0,0,48007],
'2016-09-30': [50778,0,0,0,0,0,0,0,0,0,0,0,50778],
              },
        startDate : "2016-06-01",
        endDate : "2016-09-30",
        dateFormat : "YYYY-MM-DD",
        title : "回訪率分析"};

$.fn.Retention = function (options) {
    var graphTitle = options.title || "Retention Graph";
    var data = options.data || null;

    var container = d3.select(this[0]).append("div")
        .attr("class", "box");

    var header = container.append("div")
        .attr("class", "box-header with-border");
    var title = header.append("p")
        .attr("class", "box-title")
        .text(graphTitle);
    var controls = header.append("div")
        .attr("class", "box-tools");
    var dateRange = controls.append("input")
        .attr("id", "date-range")
        .attr("type", "hidden"); //TODO: implement daterangepicker
    var switchContainer = controls.append("div")
        .attr("class", "switch-field");
    var switchData = ["days", "weeks", "months"];
    var switches = switchContainer.selectAll("span")
        .data(switchData)
        .enter()
        .append("span");
    var radios =  switches.append("input")
        .attr("type", "radio")
        .attr("name", "switch")
        .attr("id", function (d) {
            return d;
        })
        .attr("value", function (d) {
            return d;
        });
    var labelsForSwitches = switches.append("label")
        .attr("for", function (d) {
            return d;
        })
        .text(function (d) {
            return d;
        });

    var body = container.append("div")
        .attr("class", "box-body");

    var table = body.append("table")
        .attr("class", "table table-bordered text-center");

    var headData = ["Date \\ Days", "新訪客人數", "day1", "day2", "day3", "day4", "day5", "day6", "day7", "week2", "week3", "month1", "month2", "流失率"];

    var tHead = table.append("thead")
        .append("tr")
        .attr("class", "retention-thead")
        .selectAll("td")
        .data(headData)
        .enter()
        .append("td")
        .attr("class", function (d, i) {
            if(i == 0)
                return "retention-date"
            else
                return "days"
        })
        .text(function (d) {
            return d;
        });

    var rowsData = getRows(data);

    var tBody = table.append("tbody");

    var rows = tBody.selectAll("tr")
        .data(rowsData).enter()
        .append("tr");

    var cells = rows.selectAll("td")
        .data(function (row, i) {
            return row;
        }).enter()
        .append("td")
        .attr("class", function (d, i) {
            if(i == 0)
                return "retention-date";
            else
                return "days";
        })
        .attr("style", function (d, i) {
            if(i > 1)
            return "background-color :" + shadeColor("", d);
        })
        .append("div")
        .attr("data-toggle", "tooltip")
        .attr("title", function (d, i) {
            if(i != 0 && i != 1 && d != 0)
            return tooltipData(d, i);
        })
        .text(function (d, i) {
            return d + (i > 1 ? "%" : "");
        });

    $('[data-toggle="tooltip"]').tooltip(); //calling bootstrap tooltip

};
