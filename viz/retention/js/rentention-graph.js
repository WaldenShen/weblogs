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
        data: {'2016-07-24': [28767,1153,1028,573,424,384,325,263,1475,790,697,1645,20010],
'2016-07-25': [61069,2956,2622,1332,1039,683,346,609,3073,1665,1387,3172,42185],
'2016-07-26': [59813,2719,2410,1373,754,383,585,756,2827,1526,1281,2946,42253],
'2016-07-27': [61075,2635,2398,1095,489,764,989,719,2744,1621,1429,2870,43322],
'2016-07-28': [59197,2832,2054,663,971,1182,814,638,2595,1579,1166,2540,42163],
'2016-07-29': [59973,1555,1313,1770,1764,1005,704,703,2714,1515,1199,2536,43195],
'2016-07-30': [33306,1000,1161,858,520,432,345,334,1330,879,743,1471,24233],
'2016-07-31': [31402,1318,1131,586,444,362,350,269,1280,918,656,1457,22631],
'2016-08-01': [75649,3943,3174,1619,1319,831,341,579,3162,2173,1530,3144,53834],
'2016-08-02': [68376,3219,2794,1606,926,398,623,743,2987,1826,1340,2577,49337],
'2016-08-03': [65787,2993,2732,1237,478,694,815,755,2814,1771,1377,2276,47845],
'2016-08-04': [60170,2995,2122,628,870,987,827,662,2444,1531,1179,1930,43995],
'2016-08-05': [66508,1945,1421,1852,1741,1211,908,737,2639,1715,1342,2522,48475],
'2016-08-06': [34328,984,1094,790,570,439,331,251,1411,942,656,1223,25637],
'2016-08-07': [30545,1258,1116,576,469,367,245,179,1351,833,642,1103,22406],
'2016-08-08': [60692,2979,2698,1405,977,486,206,531,2594,1526,1307,1810,44173],
'2016-08-09': [59057,2837,2470,1318,613,228,564,726,2558,1439,1246,1492,43566],
'2016-08-10': [63416,3058,2720,988,292,738,930,789,2769,1729,1327,1532,46544],
'2016-08-11': [64030,2976,2069,451,1061,1131,865,705,2744,1706,1341,1236,47745],
'2016-08-12': [60489,1171,684,1760,1726,1059,770,625,2515,1522,1200,1034,46423],
'2016-08-13': [13939,107,400,361,224,169,123,136,584,378,276,258,10923],
'2016-08-14': [37598,1528,1331,737,484,406,391,292,1543,905,760,507,28714],
'2016-08-15': [66501,3146,2940,1422,963,629,376,510,2714,1693,1372,739,49997],
'2016-08-16': [66322,2941,2657,1398,793,414,590,708,2678,1772,1289,585,50497],
'2016-08-17': [64121,2683,2477,1009,540,668,804,651,2413,1642,1151,369,49714],
'2016-08-18': [61040,2645,1970,651,825,958,657,573,2304,1478,902,271,47806],
'2016-08-19': [58002,1531,1181,1597,1406,893,700,580,2304,1383,817,139,45471],
'2016-08-20': [41975,1137,1159,771,523,391,337,377,1738,1446,741,112,33243],
'2016-08-21': [34002,1290,1076,570,422,349,311,284,1348,905,544,40,26863],
'2016-08-22': [62882,2881,2665,1310,1004,523,350,506,2520,1600,665,0,48858],
'2016-08-23': [62462,2569,2337,1232,684,415,507,659,2369,1420,525,0,49745],
'2016-08-24': [60053,2568,2276,939,448,573,714,559,2417,1340,349,0,47870],
'2016-08-25': [58356,2490,1757,578,805,931,728,544,2353,1170,263,0,46737],
'2016-08-26': [54643,1404,1080,1370,1352,821,656,517,2083,1031,161,0,44168],
'2016-08-27': [34040,1050,1020,719,496,372,290,287,1322,644,102,0,27738],
'2016-08-28': [33157,1185,1091,609,435,336,266,227,1374,626,29,0,26979],
'2016-08-29': [58321,2726,2486,1241,851,505,265,474,2332,798,0,0,46643],
'2016-08-30': [61991,2598,2339,1134,591,311,542,700,2200,647,0,0,50929],
'2016-08-31': [63088,2588,2200,896,472,725,807,653,2300,501,0,0,51946],
'2016-09-01': [57733,2531,1847,606,931,1027,755,637,2110,343,0,0,46946],
'2016-09-02': [53899,1386,1077,1550,1465,876,618,534,1862,300,0,0,44231],
'2016-09-03': [31762,1001,1003,754,432,405,286,266,1014,120,0,0,26481],
'2016-09-04': [30349,1310,1069,520,382,301,306,234,971,70,0,0,25186],
'2016-09-05': [68524,3553,2957,1462,1049,738,464,395,1838,0,0,0,56068],
'2016-09-06': [62252,2940,2565,1206,814,535,455,440,1302,0,0,0,51995],
'2016-09-07': [64220,2845,2481,1251,712,546,577,560,928,0,0,0,54320],
'2016-09-08': [57047,2689,2340,799,605,670,556,358,589,0,0,0,48441],
'2016-09-09': [52957,2471,1616,992,812,726,436,254,488,0,0,0,45162],
'2016-09-10': [51262,1473,1698,1160,853,525,293,244,359,0,0,0,44657],
'2016-09-11': [30648,1094,987,603,384,271,248,208,128,0,0,0,26725],
'2016-09-12': [47049,1855,1796,791,350,297,268,137,0,0,0,0,41555],
'2016-09-13': [43552,2182,1340,480,364,294,104,0,0,0,0,0,38788],
'2016-09-14': [46591,1175,1024,579,436,224,0,0,0,0,0,0,43153],
'2016-09-15': [26818,964,779,492,199,0,0,0,0,0,0,0,24384],
'2016-09-16': [25343,864,670,259,0,0,0,0,0,0,0,0,23550],
'2016-09-17': [24587,873,446,0,0,0,0,0,0,0,0,0,23268],},
        startDate : "",
        endDate : "",
        dateFormat : "",
        title : "Retention Analysis"};

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

    var headData = ["Date \\ Days", "day0", "day1", "day2", "day3", "day4", "day5", "day6", "day7", "week2", "week3", "month1", "month2", "loss"];

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
