const state = {
  activeView: "overview",
  sgeRange: "1D",
  reversalRange: "1D",
  us10yRange: "1D",
  us10yActiveTenor: "10y",
  eventFilter: "all",
  status: null,
  reversalStatus: null,
  us10yStatus: null,
  rssMlStatus: null,
  rssMlTrainUi: {
    state: "待机",
    desc: "点击“立即训练”后会显示后端返回状态。",
    at: null,
    detail: "--",
  },
  rssMlPollTimer: null,
  notificationLogs: [],
  sgeHistory: [],
  reversalHistory: [],
  us10yHistory: [],
  charts: {},
  pagers: {},
  updateLogs: [],
  rssDedupReport: null,
  sgeYAxisRange: { min: 800, max: 1200 },
};

const UPDATE_LOG_STORAGE_KEY = "sge-monitor-update-logs";
const APP_VERSION = "0.30.5";
const PRESET_UPDATE_LOGS = [
  "2026-03-23:\u65b0\u589e\u53f3\u4e0a\u89d2\u201c\u66f4\u65b0\u8bb0\u5f55\u201d\u6309\u94ae\u4e0e\u5f39\u7a97\uff0c\u53ef\u6301\u7eed\u8bb0\u5f55\u6bcf\u6b21\u6539\u52a8\u3002",
  "2026-03-23:\u63a8\u9001\u8bbe\u7f6e\u9875\u65b0\u589e\u201c\u63a8\u9001\u8bb0\u5f55\u201d\u8868\u683c\uff0c\u5c55\u793a\u65f6\u95f4\u3001\u76ee\u6807\u3001\u7ed3\u679c\u4e0e\u5185\u5bb9\u3002",
  "2026-03-23:\u9ec4\u91d1\u53cd\u8f6c\u56db\u7ea7\u4fe1\u53f7\u89c4\u5219\uff08\u542b us10y \u6761\u4ef6\uff09\u4e0a\u7ebf\uff0c\u4ec5 1/2 \u7ea7\u63a8\u9001\u3002",
  "2026-03-23:\u83dc\u5355\u6539\u4e3a\u516d\u4e2a\u4e3b\u83dc\u5355\uff0c\u9ec4\u91d1\u53cd\u8f6c\u9884\u8b66\u4e0b\u65b0\u589e\u201c\u5341\u5e74\u671f\u7f8e\u503a\u53cd\u8f6c\u201d\u5b50\u83dc\u5355\u3002",
  "2026-03-23:\u5c06 SGE\u6ea2\u4ef7\u653e\u5165\u201c\u9ec4\u91d1\u53cd\u8f6c\u9884\u8b66\u201d\u5b50\u83dc\u5355\uff0c\u5e76\u5c06\u201c\u9ec4\u91d1\u53cd\u8f6c\u201d\u66f4\u540d\u4e3a\u201c\u76d8\u9762\u9884\u8b66\u201d\u3002",
  "2026-03-23:\u9875\u9762\u53f3\u4e0a\u65b0\u589e\u7248\u672c\u53f7\u663e\u793a\uff0c\u5f53\u524d\u7248\u672c 0.2\u3002",
  "2026-03-23:\u672c\u6b21\u6539\u52a8\u5e45\u5ea6\u8f83\u5927\uff0c\u7248\u672c\u53f7\u4ece 0.2 \u8c03\u6574\u4e3a 0.23\u3002",
  "2026-03-23:\u7f8e\u503a\u9884\u8b66\u65b0\u589e\u53ef\u914d\u7f6e\u89c4\u5219\uff1aNh\u56de\u843dNbp\uff0c\u53ef\u914d\u91c7\u6837\u9891\u7387(\u79d2)\u548c\u591a\u9009\u9650\u671f(5Y/10Y/20Y)\uff0c\u4ec5\u89e6\u53d1\u65f6\u63a8\u9001\u3002",
  "2026-03-23:\u9ec4\u91d1\u9884\u8b66\u4e0b\u65b0\u589e\u201c\u653f\u6cbb\u4e0e\u6218\u4e89\u9884\u8b66\u201d\u5b50\u83dc\u5355\uff0c\u590d\u7528RSS\u4e8b\u4ef6\u6d41\u5e76\u65b0\u589e\u201c\u9ec4\u91d1\u4e0a\u6da8\u98ce\u9669\u6253\u5206\u201d\u3002",
  "2026-03-23:\u4e8b\u4ef6\u6253\u5206\u6539\u4e3a 1-10 \u5206\u4f53\u7cfb\uff0c\u4f8b\u5982\\\"\u505c\u706b+\u964d\u606f\\\"=10\u5206\uff0c\\\"\u6218\u4e89\u5347\u7ea7+\u52a0\u606f\\\"=1\u5206\uff0c\u5e76\u8c03\u6574\u9875\u9762\u5c55\u793a\u3002",
  "2026-03-23:\u5bf9\u5df2\u5165\u5e93\u7684RSS\u4e8b\u4ef6\u6d41\u6267\u884c\u91cd\u65b0\u6253\u5206\u5e76\u56de\u5199\u6570\u636e\u5e93\u3002",
  "2026-03-23:\u6253\u5206\u65b9\u5411\u4fee\u6b63\u4e3a\\\"\u5730\u7f18\u7f13\u548c\u9ad8\u5206\uff0c\u7d27\u5f20\u5347\u7ea7\u4f4e\u5206\\\"\uff0c\u5e76\u5bf9\u5b58\u91cf\u4e8b\u4ef6\u91cd\u65b0\u6253\u5206\u3002",
  "2026-03-23:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.24.3\u3002",
  "2026-03-23:\u4fee\u590d\\\"\u6700\u540e\u901a\u7252/\u6700\u540e\u671f\u9650/\u7d27\u5f20\u5c40\u52bf\u52a0\u5267\\\"\u573a\u666f\u4f4e\u5206\u89c4\u5219\uff0c\u5e76\u91cd\u65b0\u56de\u5199\u5b58\u91cf\u4e8b\u4ef6\u8bc4\u5206\u3002",
  "2026-03-23:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.24.4\u3002",
  "2026-03-23:\u65b0\u589e\u7f8e\u503a10Y\u72ec\u7acb\u6570\u636e\u6e90\u7ef4\u62a4\uff08Sina\u4f18\u5148\uff0cFRED\u56de\u9000\uff09\uff0c\u5e76\u4e0e\u9ec4\u91d1\u8054\u52a8\u56fe\u8868\u5206\u6790\u3002",
  "2026-03-23:\u4fee\u590d\u9875\u9762\u591a\u5904\u4e71\u7801\u4e0e\u5b57\u7b26\u663e\u793a\u4e0d\u5168\u95ee\u9898\u3002",
  "2026-03-23:\u540e\u7aef\u65b0\u589eRSS\u4e8b\u4ef6-\\\"\u91d1\u4ef7/\u6da8\u8dcc\u5e45\\\"\u6837\u672c\u5e93\uff0c\u6bcf\u6ee1100\u6761\u6267\u884c\u4e00\u6b215\u5c42\u795e\u7ecf\u7f51\u7edc\u8bad\u7ec3\uff08lr=0.001\uff0c\u4f59\u5f26\u9000\u706b\uff0cearly-stop=25\uff09\u5e76\u5bf9\u65b0RSS\u81ea\u52a8ML\u6253\u5206\uff08\u524d\u7aef\u73b0\u6709\u8bc4\u5206\u4fdd\u6301\u4e0d\u53d8\uff09\u3002",
  "2026-03-23:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.25.0\u3002",
  "2026-03-23:\u653f\u6cbb\u4e0e\u6218\u4e89\u9884\u8b66\u9875\u9762\u65b0\u589e ML \u6a21\u578b\u53c2\u6570\u914d\u7f6e\u3001\u8bad\u7ec3\u635f\u5931\u53ef\u89c6\u5316\u3001\u624b\u52a8\u5f3a\u5236\u8bad\u7ec3\u4e0e RSS \u5168\u91cf\u6d4b\u8bd5\u6293\u53d6\u5165\u5e93\u80fd\u529b\u3002",
  "2026-03-23:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.26.0\u3002",
  "2026-03-23:\u4fee\u590d\u201c\u5168\u91cf\u6293\u53d6\u6d4b\u8bd5\u201d404\u63a5\u53e3\u4e0e\u5165\u5e93\u903b\u8f91\uff0c\u6539\u4e3a\u4f7f\u7528\u5168\u90e8RSS\u6e90\u5e76\u652f\u6301\u5168\u91cf\u5b58\u50a8\uff08\u4e0d\u4ec5\u589e\u91cf\u53bb\u91cd\uff09\u3002",
  "2026-03-23:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.26.1\u3002",
  "2026-03-23:\u653f\u6cbb\u4e0e\u6218\u4e89\u9884\u8b66\u9875\u65b0\u589e\u201c\u6a21\u578b\u8bad\u7ec3\u72b6\u6001\u201d\u533a\u57df\uff0c\u5b9e\u65f6\u6620\u5c04\u540e\u7aef\u8fd4\u56de\uff08\u8fdb\u884c\u4e2d/\u6210\u529f/\u672a\u89e6\u53d1/\u5931\u8d25\uff09\u4e0e\u81ea\u52a8\u8bad\u7ec3\u8fdb\u5ea6\u3002",
  "2026-03-23:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.26.2\u3002",
  "2026-03-23:\u8bad\u7ec3\u53ef\u89c6\u5316\u6539\u4e3a\u201c\u6bcf\u8f6e(epoch)\u66f2\u7ebf\u201d\uff0c\u524d\u7aef\u53ef\u663e\u793a\u4e00\u6b21\u8bad\u7ec3\u7684\u5b8c\u6574 train/val loss \u8fc7\u7a0b\u3002",
  "2026-03-23:RSS\u6e90\u914d\u7f6e\u652f\u6301\u300c\u5907\u6ce8\u540d\u79f0+\u542f\u7528\u5f00\u5173+\u5730\u5740\u300d\uff0c\u5168\u91cf\u6293\u53d6\u4ec5\u5bf9\u542f\u7528\u6e90\u751f\u6548\u3002",
  "2026-03-23:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.27.0\u3002",
  "2026-03-23:\u6a21\u578b\u8bad\u7ec3\u56fe\u8868\u65b0\u589e\u51c6\u786e\u7387\u66f2\u7ebf\uff08Train/Val Accuracy\uff09\u4e0e\u6700\u65b0\u51c6\u786e\u7387\u663e\u793a\u3002",
  "2026-03-23:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.27.1\u3002",
  "2026-03-23:\u65b0\u589e\u201c\u6e05\u7a7a\u6a21\u578b\u6837\u672c\u201d\u6309\u94ae\uff0c\u53ef\u4e00\u952e\u6e05\u7a7a RSS ML \u6837\u672c\u3001\u8bad\u7ec3\u8bb0\u5f55\u4e0e\u6a21\u578b\u6587\u4ef6\u3002",
  "2026-03-23:\u8bad\u7ec3\u5931\u8d25\u65f6\u524d\u7aef\u663e\u793a\u8be6\u7ec6\u9519\u8bef\u4fe1\u606f\uff08\u7c7b\u578b/\u6d88\u606f/traceback \u5c3e\u90e8\uff09\u3002",
  "2026-03-25:\u6a21\u578b\u8bad\u7ec3\u6539\u4e3a\u4ec5\u4f7f\u7528\u300c\u65b0\u95fb\u6807\u9898\u300d\u5355\u5217\u8f93\u5165\uff0c\u5e76\u5347\u7ea7\u4e3a\u56db\u7c7b\u805a\u7c7b\u76ee\u6807\uff08\u5927/\u5c0f\u5e45\u5229\u597d\u3001\u5c0f/\u5927\u5e45\u5229\u7a7a\u9ec4\u91d1\uff09\u3002",
  "2026-03-25:\u524d\u7aef\u652f\u6301\u8bad\u7ec3\u8fc7\u7a0b\u5b9e\u65f6\u66f2\u7ebf\uff08loss+accuracy\uff09\u3001\u53ef\u968f\u65f6\u6682\u505c/\u7ee7\u7eed/\u53d6\u6d88\u8bad\u7ec3\uff0c\u5e76\u5b9e\u65f6\u663e\u793a\u540e\u7aef\u8bad\u7ec3\u72b6\u6001\u3002",
  "2026-03-25:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.28.0\u3002",
  "2026-03-25:\u653f\u6cbb\u4e0e\u6218\u4e89\u9884\u8b66\u53d6\u6d88\u300c\u7f13\u548c\u8bc4\u5206\u300d\uff0c\u6539\u4e3a\u5c55\u793a RSS ML \u56db\u5206\u7c7b\uff08\u5927\u5e45\u5229\u597d/\u5c0f\u5e45\u5229\u597d/\u5c0f\u5e45\u5229\u7a7a/\u5927\u5e45\u5229\u7a7a\uff09\u5206\u503c\u4e0e\u6982\u7387\u3002",
  "2026-03-25:RSS ML \u8bad\u7ec3\u72b6\u6001\u589e\u52a0\u201c\u53d6\u6d88\u4e2d\u201d\u5b9e\u65f6\u53cd\u9988\uff0c\u524d\u7aef\u4f1a\u7acb\u5373\u663e\u793a\u8bf7\u6c42\u5df2\u53d1\u9001\u5e76\u6301\u7eed\u8f6e\u8be2\u76f4\u5230\u4efb\u52a1\u7ec8\u6001\u3002",
  "2026-03-25:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.28.1\u3002",
  "2026-03-25:RSS ML \u8bad\u7ec3\u65b0\u589e\u201c\u7c7b\u522b\u6743\u91cd+\u91cd\u91c7\u6837+\u65f6\u95f4\u8870\u51cf\u201d\uff0c\u7f13\u89e3\u7c7b\u522b\u5854\u7f29\u3002",
  "2026-03-25:\u65b0\u589e\u201c\u672a\u6765\u7a97\u53e3\u6253\u6807\u201d\uff081h/4h/24h\u53ef\u914d\uff09\uff0c\u5e76\u53ef\u5207\u6362\u4e3a\u624b\u5de5\u8bc4\u5206\u6253\u6807\u3002",
  "2026-03-25:\u4e3b\u6307\u6807\u5207\u6362\u4e3a Macro-F1\uff0c\u5e76\u5728\u53ef\u89c6\u5316\u91cc\u663e\u793a\u6bcf\u7c7b Precision/Recall/F1 \u4e0e\u6df7\u6dc6\u77e9\u9635\u3002",
  "2026-03-25:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.29.0\u3002",
  "2026-03-26:\u653f\u6cbb\u4e0e\u6218\u4e89\u9884\u8b66\u533a\u57df\u65b0\u589e\u300c\u624b\u52a8\u53bb\u91cd\u300d\u6309\u94ae\uff0c\u53ef\u4e00\u952e\u6267\u884c RSS \u8bed\u4e49\u53bb\u91cd\u5e76\u540c\u6b65 CSV\u3002",
  "2026-03-26:\u65b0\u589e\u300c\u53bb\u91cd\u62a5\u544a\u9762\u677f\u300d\uff0c\u5c55\u793a\u672c\u8f6e\u53bb\u91cd\u6570\u91cf\uff08\u4e8b\u4ef6/\u6837\u672c/\u5206\u7ec4\uff09\u4e0e\u91cd\u590d\u6700\u591a\u7684\u6765\u6e90\u6392\u884c\u3002",
  "2026-03-26:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.29.4\u3002",
  "2026-03-26:RSS\u53bb\u91cd\u5347\u7ea7\u4e3a\u300c\u7cbe\u786e+\u6a21\u7cca\u300d\u4e24\u9636\u6bb5\uff0c\u652f\u6301\u8fd1\u4e49\u6807\u9898\u53bb\u91cd\uff08\u5982\u201c\u8239\u53ea/\u8239\u5458\u201d\u3001\u201cLNG\u8f6e/\u6cb9\u8f6e\u201d\uff09\u3002",
  "2026-03-26:CSV\u540c\u6b65\u9636\u6bb5\u65b0\u589e\u8fd1\u4e49\u53bb\u91cd\uff0c\u9632\u6b62\u8bad\u7ec3\u6837\u672c\u91cd\u590d\u5f71\u54cd\u6743\u91cd\u3002",
  "2026-03-26:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.29.5\u3002",
  "2026-03-26:RSS ML \u6837\u672cCSV\u652f\u6301\u65b0\u7ed3\u6784\u5217 D_score(\u6620\u5c04\u5206\u503c) \u4e0e E_reasoning(\u6253\u5206\u539f\u56e0)\uff0c\u8bad\u7ec3\u4f18\u5148\u8bfb\u53d6 D_score \u4e0e class_label\u3002",
  "2026-03-26:CSV \u540c\u6b65\u4fdd\u7559\u4eba\u5de5\u6807\u6ce8\u5217\uff08class_label/D_score/E_reasoning\uff09\uff0c\u907f\u514d\u88ab\u81ea\u52a8\u540c\u6b65\u8986\u76d6\u3002",
  "2026-03-26:CSV\u4eba\u5de5\u6807\u6ce8\u6a21\u5f0f\u4e0b\uff0c\u8bad\u7ec3\u4f18\u5148\u4f7f\u7528 D_score/class_label\uff0c\u4e0d\u518d\u56de\u843d\u5230\u81ea\u52a8 target_score\u3002",
  "2026-03-26:RSS\u6293\u53d6\u4e0e\u624b\u52a8\u53bb\u91cd\u4e0d\u518d\u9ed8\u8ba4\u5168\u91cf\u8986\u76d6CSV\uff0c\u907f\u514d\u4eba\u5de5\u53bb\u91cd\u540e\u88ab\u56de\u586b\u3002",
  "2026-03-26:\u6bcf\u6b21\u70b9\u51fb\u8bad\u7ec3\u524d\uff0c\u540e\u7aef\u81ea\u52a8\u5148\u6267\u884c CSV->DB \u540c\u6b65\u4e00\u904d\u3002",
  "2026-03-26:\u8bad\u7ec3\u72b6\u6001\u63a5\u53e3\u65b0\u589e last_csv_db_sync \u4fe1\u606f\uff0c\u53ef\u7528\u4e8e\u6838\u5bf9\u672c\u6b21\u540c\u6b65\u6761\u6570\u3002",
  "2026-03-26:\u53cd\u8f6c\u9884\u8b66\u63a8\u9001\u52a0\u56fa\uff1a\u4e3b\u5faa\u73af\u4ec5\u5728 signal_level \u4e3a 1/2 \u65f6\u624d\u4f1a\u8fdb\u5165\u63a8\u9001\u6d41\u7a0b\u3002",
  "2026-03-26:DingTalk\u53d1\u9001\u5c42\u589e\u52a0\u4fdd\u5e95\u62e6\u622a\uff1a\u5305\u542b\u300c\u9ec4\u91d1\u53cd\u8f6c\u4e09\u7ea7/\u56db\u7ea7\u4fe1\u53f7\u300d\u6587\u6848\u7684\u6d88\u606f\u4e0d\u4f1a\u53d1\u51fa\u3002",
  "2026-03-26:\u63a8\u9001\u9632\u7ebf\u518d\u52a0\u56fa\uff1a\u4efb\u4f55\u542b\u2018\u8b66\u62a5\u7b49\u7ea7: 3\u7ea7/4\u7ea7\u2019\u7684\u6d88\u606f\u5747\u88ab\u62e6\u622a\u4e0d\u53d1\u9001\u3002",
  "2026-03-26:\u6838\u5bf9\u4e86 2026-03-26 \u8bb0\u5f55\uff1a\u672c\u5730\u6570\u636e\u5e93\u65e0\u65b0\u7684 reversal_alert/notification_logs \u53d1\u9001\u8bb0\u5f55\uff0c\u7591\u4f3c\u5916\u90e8\u8fdb\u7a0b\u6cbf\u7528\u76f8\u540c webhook \u53d1\u9001\u3002",
  "2026-03-26:RSS \u6837\u672c\u6d41\u7a0b\u62c6\u5206\u4e3a\u4e24\u4e2aCSV\uff1a\u6293\u53d6\u4e8b\u4ef6CSV(rss_fetched_events_sync.csv) \u4e0e\u8bad\u7ec3\u6807\u6ce8CSV(rss_ml_samples_sync.csv)\u3002",
  "2026-03-26:\u6bcf\u6b21\u70b9\u51fb\u300c\u7acb\u5373\u8bad\u7ec3\u300d\uff0c\u540e\u7aef\u4f1a\u5148\u6267\u884c CSV->DB \u540c\u6b65\uff0c\u518d\u542f\u52a8\u8bad\u7ec3\u3002",
  "2026-03-26:\u8bad\u7ec3\u6210\u529f\u540e\u81ea\u52a8\u5bf9\u672a\u6253\u5206RSS\u4e8b\u4ef6\u6267\u884c\u6a21\u578b\u56de\u586b\u6253\u5206\uff0c\u5e76\u5237\u65b0\u6293\u53d6CSV\u3002",
  "2026-03-26:\u4fee\u590dRSS ML\u8bad\u7ec3\u9762\u677f\u4e0e\u9ec4\u91d1-\u7f8e\u503a\u8054\u52a8\u56fe\u8868\u4e2d\u7684\u5360\u4f4d\u7b26\u4e71\u7801\uff0c\u7edf\u4e00\u4e2d\u6587\u72b6\u6001\u6587\u6848\u3002",
  "2026-03-26:RSS ML \u8bad\u7ec3\u5b8c\u6210\u7ed3\u679c\u589e\u52a0 epochs_ran \u5c55\u793a\uff0c\u907f\u514d\u628a best_epoch \u8bef\u89e3\u4e3a\u5b9e\u9645\u8bad\u7ec3\u8f6e\u6b21\u3002",
  "2026-03-26:\u4fee\u590d\u5341\u5e74\u671f\u7f8e\u503a\u5206\u65f6\u6e90\u89e3\u6790\uff0cEastmoney \u6539\u7528 f86 \u65f6\u95f4\u6233\u505a\u65b0\u9c9c\u5ea6\u6821\u9a8c\uff0c\u6062\u590d\u5206\u65f6\u66f4\u65b0\u5e76\u53bb\u9664\u91cd\u590d FAIL \u72b6\u6001\u3002",
  "2026-03-26:\u5341\u5e74\u671f\u7f8e\u503a\u9875\u9762\u56fe\u8868\u4e0e\u8bad\u7ec3\u72b6\u6001\u9762\u677f\u5360\u4f4d\u7b26\u4e71\u7801\u4fee\u590d\uff1b\u9759\u6001\u8d44\u6e90\u589e\u52a0\u7248\u672c\u53c2\u6570\u9632\u7f13\u5b58\u3002",
  "2026-03-26:RSS ML \u5b66\u4e60\u7387\u8f93\u5165\u6539\u4e3a step=any\uff0c\u4fee\u590d\u6d4f\u89c8\u5668\u201c\u6700\u8fd1\u6709\u6548\u503c\u201d\u62e6\u622a\u95ee\u9898\uff08\u53ef\u8f93\u5165 0.0001 / 0.001 \u7b49\uff09\u3002",
  "2026-03-26:RSS ML \u5b66\u4e60\u7387\u540e\u7aef\u8303\u56f4\u653e\u5bbd\u4e3a 0.000001 ~ 1.0\uff0c\u652f\u6301\u66f4\u5927\u7684\u8c03\u53c2\u533a\u95f4\u3002",
  "2026-03-26:\u7f8e\u503a\u9884\u8b66\u65b0\u589e\u51b7\u5374\u65f6\u95f4\u9632\u6296\uff08\u9ed8\u8ba4 1800 \u79d2\uff09\uff0c\u91cd\u542f\u670d\u52a1\u65f6\u4e0d\u518d\u8fde\u7eed\u91cd\u590d\u63a8\u9001\u3002",
  "2026-03-26:\u65b0\u589e\u7f8e\u503a\u201c\u9884\u8b66\u51b7\u5374(\u79d2)\u201d\u53c2\u6570\uff0c\u53ef\u5728\u9875\u9762\u76f4\u63a5\u8c03\u6574\u63a8\u9001\u9891\u7387\u3002",
  "2026-03-26:\u4fee\u590d\u901a\u7528\u63a8\u9001\u7b49\u7ea7\u62e6\u622a\u6b63\u5219\uff0c\u4fee\u590d\\\"RSS\u4e8b\u4ef6\u8b66\u62a5 3\u7ea7/4\u7ea7\\\"\u6f0f\u62e6\u622a\u95ee\u9898\u3002",
  "2026-03-26:\u65b0\u589e\u63a8\u9001\u7ec8\u7aef\u9632\u7ebf\uff1a\u201cRSS\u4e8b\u4ef6\u8b66\u62a5 + \u7b49\u7ea7>=3\u201d\u4e00\u5f8b\u4e0d\u53d1\u9001\u3002",
  "2026-03-26:\u63a8\u9001\u7ec8\u7aef\u589e\u52a0\\\"\u65e7\u7248RSS\u4e8b\u4ef6\u8b66\u62a5\\\"\u5168\u91cf\u62e6\u622a\u5f00\u5173\uff0c\u9632\u6b62\u91cd\u542f\u540e\u591a\u6761\u5386\u53f2RSS\u4e8b\u4ef6\u88ab\u6279\u91cf\u63a8\u9001\u3002",
  "2026-03-26:\u7248\u672c\u53f7\u66f4\u65b0\u4e3a 0.30.5\u3002",
];

function isLikelyGarbledText(text) {
  if (!text) return false;
  return /[?]{2,}|[\ufffd\uE000-\uF8FF]/.test(text);
}

function escapeHtml(text) {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function loadUpdateLogs() {
  try {
    const raw = localStorage.getItem(UPDATE_LOG_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter((item) => item && item.text && item.created_at)
      .filter((item) => !isLikelyGarbledText(String(item.text)));
  } catch (error) {
    return [];
  }
}

function saveUpdateLogs() {
  localStorage.setItem(UPDATE_LOG_STORAGE_KEY, JSON.stringify(state.updateLogs));
}

function appendUpdateLog(text) {
  const content = String(text || "").trim();
  if (!content) return;
  state.updateLogs.unshift({
    text: content,
    created_at: new Date().toISOString(),
  });
  saveUpdateLogs();
  renderUpdateLogList();
}

function ensurePresetUpdateLogs() {
  const existingTexts = new Set((state.updateLogs || []).map((item) => String(item.text || "").trim()));
  let changed = false;
  for (const text of PRESET_UPDATE_LOGS) {
    if (existingTexts.has(text)) continue;
    state.updateLogs.unshift({
      text,
      created_at: new Date().toISOString(),
    });
    changed = true;
  }
  if (changed) {
    saveUpdateLogs();
  }
}

function renderUpdateLogList() {
  const list = document.getElementById("updateLogList");
  if (!list) return;
  if (!state.updateLogs.length) {
    list.innerHTML = `<div class="empty-state">\u6682\u65e0\u66f4\u65b0\u8bb0\u5f55</div>`;
    return;
  }
  list.innerHTML = state.updateLogs
    .map(
      (item) => `
    <article class="update-log-item">
      <span class="update-log-time">${formatTime(item.created_at)}</span>
      <div class="update-log-text">${escapeHtml(item.text)}</div>
    </article>
  `,
    )
    .join("");
}

function openUpdateLogModal() {
  const modal = document.getElementById("updateLogModal");
  if (!modal) return;
  renderUpdateLogList();
  modal.classList.remove("hidden");
}

function closeUpdateLogModal() {
  const modal = document.getElementById("updateLogModal");
  if (!modal) return;
  modal.classList.add("hidden");
}

const pageSizes = {
  overviewSgeAlerts: 4,
  overviewReversalAlerts: 4,
  overviewEvents: 4,
  samples: 6,
  alerts: 6,
  reversalSamples: 6,
  reversalAlerts: 6,
  feedEvents: 5,
  rssFetchRuns: 6,
  notificationLogs: 6,
  fetchRuns: 6,
  reversalRuns: 6,
  us10ySamples: 8,
  us10yRuns: 8,
  geoEvents: 8,
};

const viewMeta = {
  overview: {
    title: "\u603b\u89c8",
    desc: "\u540c\u65f6\u67e5\u770b SGE \u6ea2\u4ef7\u3001\u9ec4\u91d1\u53cd\u8f6c\u7b49\u7ea7\u3001RSS \u547d\u4e2d\u548c\u63a8\u9001\u72b6\u6001\u3002",
  },
  goldWarning: {
    title: "\u9ec4\u91d1\u53cd\u8f6c\u9884\u8b66",
    desc: "\u4e0b\u7ea7\u83dc\u5355\u5305\u542b\u9ec4\u91d1\u53cd\u8f6c\u4e0e\u5341\u5e74\u671f\u7f8e\u503a\u53cd\u8f6c\u8054\u52a8\u5206\u6790\u3002",
  },
  sge: {
    title: "SGE\u6ea2\u4ef7",
    desc: "\u76d1\u63a7\u4eba\u6c11\u5e01\u91d1\u4ef7\u4e0e\u56fd\u9645\u91d1\u6298\u7b97\u4ef7\u5dee\uff0c\u5e76\u89e6\u53d1\u9608\u503c\u9884\u8b66\u3002",
  },
  reversal: {
    title: "\u76d8\u9762\u9884\u8b66",
    desc: "\u76d8\u9762 + RSS + us10y \u8054\u5408\u6253\u5206\uff0c\u8f93\u51fa 1 / 2 / 3 / 4 \u7ea7\u4fe1\u53f7\u3002",
  },
  us10y: {
    title: "\u5341\u5e74\u671f\u7f8e\u503a\u53cd\u8f6c",
    desc: "\u72ec\u7acb\u6570\u636e\u6e90\u7ef4\u62a4\uff0c\u4e0e\u9ec4\u91d1\u4ef7\u683c\u8054\u52a8\u5206\u6790\u3002",
  },
  geoWarning: {
    title: "\u653f\u6cbb\u4e0e\u6218\u4e89\u9884\u8b66",
    desc: "\u57fa\u4e8e RSS \u4e8b\u4ef6\u6d41\u5bf9\u9ec4\u91d1\u4e0a\u6da8\u98ce\u9669\u8fdb\u884c\u6253\u5206\u3002",
  },
  otherWarning: {
    title: "\u5176\u4ed6\u9884\u8b66",
    desc: "\u9884\u7559\u6a21\u5757\uff0c\u540e\u7eed\u53ef\u6269\u5c55\u3002",
  },
  feeds: {
    title: "RSS \u6e90",
    desc: "\u914d\u7f6e\u65b0\u95fb\u6e90\u5e76\u67e5\u770b\u4e8b\u4ef6\u5206\u7c7b\u548c\u6293\u53d6\u8d28\u91cf\u3002",
  },
  push: {
    title: "\u63a8\u9001\u8bbe\u7f6e",
    desc: "\u7ba1\u7406 webhook + secret \u63a8\u9001\u76ee\u6807\u5e76\u8bb0\u5f55\u63a8\u9001\u7ed3\u679c\u3002",
  },
  system: {
    title: "\u7cfb\u7edf\u72b6\u6001",
    desc: "\u67e5\u770b SGE\u3001\u53cd\u8f6c\u4e0e RSS \u8c03\u5ea6\u8fd0\u884c\u72b6\u6001\u3002",
  },
};

function initCharts() {
  const chartIds = ["sgeChart", "sgeDetailChart", "reversalChart", "reversalDetailChart", "us10yLinkChart", "rssMlTrainChart"];
  chartIds.forEach((id) => {
    const node = document.getElementById(id);
    if (!node) return;
    state.charts[id] = echarts.init(node, null, { renderer: "canvas" });
  });
  bindSgeYAxisZoom(state.charts.sgeChart);
  bindSgeYAxisZoom(state.charts.sgeDetailChart);
}

function resetSgeYAxisRange() {
  state.sgeYAxisRange = { min: 800, max: 1200 };
}

function clampSgeYAxisRange(min, max) {
  const safeMin = Number.isFinite(min) ? min : 800;
  const safeMax = Number.isFinite(max) ? max : 1200;
  const span = Math.max(20, safeMax - safeMin);
  return {
    min: Number(safeMin.toFixed(2)),
    max: Number((safeMin + span).toFixed(2)),
  };
}

function renderSgeChartsOnly() {
  renderSgeChart(state.charts.sgeChart);
  renderSgeChart(state.charts.sgeDetailChart);
}

function zoomSgeYAxisByWheel(delta, anchorRatio = 0.5) {
  const current = state.sgeYAxisRange || { min: 800, max: 1200 };
  const span = Math.max(20, current.max - current.min);
  const factor = delta > 0 ? 0.9 : 1.1;
  const newSpan = Math.min(4000, Math.max(20, span * factor));
  const ratio = Math.min(1, Math.max(0, anchorRatio));
  const center = current.min + span * ratio;
  const nextMin = center - newSpan * ratio;
  const nextMax = center + newSpan * (1 - ratio);
  state.sgeYAxisRange = clampSgeYAxisRange(nextMin, nextMax);
  renderSgeChartsOnly();
}

function bindSgeYAxisZoom(chart) {
  if (!chart || chart.__sgeYAxisWheelBound) return;
  chart.__sgeYAxisWheelBound = true;
  chart.getZr().on("mousewheel", (params) => {
    const nativeEvent = params?.event?.event;
    const wheelDelta = params?.event?.wheelDelta ?? (nativeEvent ? -nativeEvent.deltaY : 0);
    if (!wheelDelta) return;
    const height = Math.max(1, chart.getHeight());
    const offsetY = Number(params?.offsetY ?? height / 2);
    const anchorRatio = 1 - Math.min(1, Math.max(0, offsetY / height));
    zoomSgeYAxisByWheel(wheelDelta, anchorRatio);
    if (nativeEvent && typeof nativeEvent.preventDefault === "function") {
      nativeEvent.preventDefault();
    }
  });
  chart.getZr().on("dblclick", () => {
    resetSgeYAxisRange();
    renderSgeChartsOnly();
  });
}

function formatNumber(value, digits = 4) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return Number(value).toFixed(digits);
}

function formatTime(iso) {
  if (!iso) return "--";
  return new Date(iso).toLocaleString("zh-CN", { hour12: false });
}

function toChartTimestamp(value) {
  const time = new Date(value).getTime();
  return Number.isFinite(time) ? time : null;
}

function findNearestSeriesItem(items, targetTime) {
  if (!Array.isArray(items) || !items.length || !Number.isFinite(targetTime)) return null;
  let bestItem = null;
  let bestDelta = Number.POSITIVE_INFINITY;
  for (const item of items) {
    const itemTime = toChartTimestamp(item?.fetched_at);
    if (itemTime === null) continue;
    const delta = Math.abs(itemTime - targetTime);
    if (delta < bestDelta) {
      bestDelta = delta;
      bestItem = item;
    }
  }
  return bestItem;
}

function buildUs10yTooltip(axisValue, goldItems, us10yItemsByTenor, tenors, tenorColor) {
  const axisTime = toChartTimestamp(axisValue);
  const goldSample = findNearestSeriesItem(goldItems, axisTime);
  const headerTime = goldSample?.fetched_at || axisValue;
  const rows = [
    `<div><strong>${escapeHtml(formatTime(headerTime))}</strong></div>`,
    `<div><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#13796b;margin-right:6px;"></span>黄金: ${escapeHtml(formatNumber(goldSample?.gold_price_usd_per_oz, 2))}</div>`,
  ];
  tenors.forEach((tenor) => {
    const sample = findNearestSeriesItem(us10yItemsByTenor[tenor] || [], axisTime);
    const label = `美债${tenor.toUpperCase()}`;
    rows.push(
      `<div><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${tenorColor[tenor] || "#c04f2d"};margin-right:6px;"></span>${escapeHtml(label)}: ${escapeHtml(formatNumber(sample?.yield_pct, 3))}%</div>`,
    );
  });
  return rows.join("");
}

function formatLevel(level) {
  const map = {
    0: "无信号",
    1: "一级",
    2: "二级",
    3: "三级",
    4: "四级",
  };
  return map[level] || "无信号";
}

function levelClass(level) {
  return `level-${level ?? 0}`;
}

function getActiveTargets(settings = {}) {
  const targets = settings.notification_targets || [];
  return targets.filter((item) => item.enabled);
}

function setText(id, text) {
  const node = document.getElementById(id);
  if (node) node.textContent = text;
}

function getToastHost() {
  let host = document.getElementById("toastHost");
  if (!host) {
    host = document.createElement("div");
    host.id = "toastHost";
    host.className = "toast-host";
    document.body.appendChild(host);
  }
  return host;
}

function showToast(title, message, tone = "success") {
  const host = getToastHost();
  const toast = document.createElement("div");
  toast.className = `toast ${tone}`;
  toast.innerHTML = `<span class="toast-title">${title}</span><span>${message}</span>`;
  host.appendChild(toast);
  window.setTimeout(() => {
    toast.remove();
  }, 3200);
}

function formatErrorMessage(error) {
  if (!error) return "请求失败";
  const raw = String(error.message || error);
  try {
    const parsed = JSON.parse(raw);
    if (parsed?.detail) {
      if (typeof parsed.detail === "string") return parsed.detail.slice(0, 500);
      return JSON.stringify(parsed.detail).slice(0, 500);
    }
  } catch (e) {}
  return raw.slice(0, 500);
}

function getCurrentPage(key, totalItems, pageSize) {
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  const current = state.pagers[key] || 1;
  if (current > totalPages) {
    state.pagers[key] = totalPages;
    return totalPages;
  }
  return current;
}

function getPagedItems(items, key) {
  const pageSize = pageSizes[key] || 6;
  const page = getCurrentPage(key, items.length, pageSize);
  const start = (page - 1) * pageSize;
  return {
    items: items.slice(start, start + pageSize),
    page,
    pageSize,
    totalPages: Math.max(1, Math.ceil(items.length / pageSize)),
    totalItems: items.length,
  };
}

function renderPager(pagerId, key, totalItems) {
  const node = document.getElementById(pagerId);
  if (!node) return;
  const pageSize = pageSizes[key] || 6;
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  if (totalItems <= pageSize) {
    node.innerHTML = "";
    return;
  }
  const page = getCurrentPage(key, totalItems, pageSize);
  const buttons = [];
  const addPageButton = (p) => {
    buttons.push(`<button class="pager-btn ${p === page ? "active" : ""}" data-pager="${key}" data-page="${p}">${p}</button>`);
  };
  const addEllipsis = () => {
    buttons.push(`<span class="pager-ellipsis">...</span>`);
  };
  const pages = [];
  if (totalPages <= 9) {
    for (let i = 1; i <= totalPages; i += 1) pages.push(i);
  } else {
    pages.push(1, 2);
    const left = Math.max(3, page - 1);
    const right = Math.min(totalPages - 2, page + 1);
    if (left > 3) pages.push(-1);
    for (let i = left; i <= right; i += 1) pages.push(i);
    if (right < totalPages - 2) pages.push(-1);
    pages.push(totalPages - 1, totalPages);
  }
  buttons.push(`<button class="pager-btn" data-pager="${key}" data-page="${page - 1}" ${page <= 1 ? "disabled" : ""}>上一页</button>`);
  pages.forEach((p) => {
    if (p === -1) addEllipsis();
    else addPageButton(p);
  });
  buttons.push(`<button class="pager-btn" data-pager="${key}" data-page="${page + 1}" ${page >= totalPages ? "disabled" : ""}>下一页</button>`);
  node.innerHTML = `<span class="pager-meta">第 ${page} / ${totalPages} 页，共 ${totalItems} 条</span>${buttons.join("")}`;
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Request failed: ${res.status}`);
  }
  return res.json();
}

async function fetchJsonOptional(url, options = {}, fallback = {}) {
  try {
    return await fetchJson(url, options);
  } catch (error) {
    return fallback;
  }
}

async function refreshAll() {
  const reversalStride = state.reversalRange === "1W" ? 20 : 1;
  const [status, sgeHistory, reversalStatus, reversalHistory, us10yStatus, us10yHistory, notificationLogs, rssMlStatus] = await Promise.all([
    fetchJson("/api/status"),
    fetchJson(`/api/history?range=${state.sgeRange}`),
    fetchJson("/api/reversal/status"),
    fetchJson(`/api/reversal/history?range=${state.reversalRange}&stride=${reversalStride}`),
    fetchJson("/api/us10y/status"),
    fetchJson(`/api/us10y/history?range=${state.us10yRange}&stride=20`),
    fetchJsonOptional("/api/notification/logs?limit=120", {}, { items: [] }),
    fetchJsonOptional("/api/rss-ml/status", {}, null),
  ]);
  state.status = status;
  state.reversalStatus = reversalStatus;
  state.us10yStatus = us10yStatus;
  state.sgeHistory = sgeHistory.items || [];
  state.reversalHistory = reversalHistory.items || [];
  state.us10yHistory = us10yHistory.items || [];
  state.notificationLogs = notificationLogs.items || [];
  state.rssMlStatus = rssMlStatus;
  renderAll();
  if (state.rssMlStatus?.runtime?.running) {
    startRssMlTrainPolling();
  } else {
    stopRssMlTrainPolling();
  }
}

function renderAll() {
  if (!state.status || !state.reversalStatus || !state.us10yStatus) return;
  const settings = state.status.settings || {};
  const marketState = state.status.market_state || {};
  const reversalLatest = state.reversalStatus.latest_sample;
  const latest = state.status.latest_sample;
  const activeTargets = getActiveTargets(settings);
  const rssRuns = state.reversalStatus.recent_rss_fetch_runs || [];
  const lastRssRun = rssRuns[0];
  const us10yTenors = settings.us10y_tenors || ["10y"];
  state.us10yActiveTenor = us10yTenors.includes("10y") ? "10y" : us10yTenors[0];
  const latestSamples = state.us10yStatus.latest_samples || {};
  const latestUs10y = latestSamples[state.us10yActiveTenor] || state.us10yStatus.latest_sample;
  const tenorLabel = (us10yTenors || []).map((item) => String(item).toUpperCase()).join("/") || "10Y";

  setText("schedulerBadge", state.status.scheduler?.running ? "运行中" : "已停止");
  setText("nextRunText", formatTime(state.status.scheduler?.next_run_time));
  setText("overviewPremiumValue", latest?.premium_cny_per_g != null ? `${formatNumber(latest.premium_cny_per_g, 4)} 元/克` : "无数据");
  setText("overviewPremiumMeta", latest ? `${formatTime(latest.fetched_at)} | ${latest.note || ""}` : "等待数据");
  setText("overviewSignalValue", formatLevel(reversalLatest?.signal_level ?? 0));
  setText("overviewSignalMeta", reversalLatest ? `${formatTime(reversalLatest.fetched_at)} | ${reversalLatest.note || ""}` : "等待数据");
  setText("overviewConditionsValue", reversalLatest?.triggered_conditions || "--");
  setText("overviewConditionsMeta", reversalLatest?.note || "price / political / war / us10y");
  setText("overviewFeedCount", String((settings.rss_feed_urls || []).length));
  setText("overviewFeedMeta", `RSS \u9891\u7387 ${settings.rss_poll_interval_seconds ?? "--"} \u79d2`);
  setText("overviewTargetCount", String(activeTargets.length));
  setText("overviewTargetMeta", activeTargets.length ? activeTargets.map((item) => item.name).join(" / ") : "未配置");
  setText("overviewRssValue", lastRssRun ? `${lastRssRun.item_count} 条` : "--");
  setText("overviewRssMeta", lastRssRun ? `${formatTime(lastRssRun.fetched_at)} | ${lastRssRun.success ? "成功" : lastRssRun.error_message || "失败"}` : "等待数据");
  setText("systemSgeState", marketState.sge?.label || "--");
  setText("systemSgeMeta", marketState.sge?.detail || "\u7b49\u5f85\u6570\u636e");
  setText("systemReversalState", marketState.reversal?.label || "--");
  setText("systemReversalMeta", marketState.reversal?.detail || "\u7b49\u5f85\u6570\u636e");
  setText("systemRssState", marketState.rss?.label || "--");
  setText("systemRssMeta", marketState.rss?.detail || "\u7b49\u5f85\u6570\u636e");

  setText("reversalLevelValue", formatLevel(reversalLatest?.signal_level ?? 0));
  setText("reversalLevelMeta", reversalLatest ? `${formatTime(reversalLatest.fetched_at)} | ${reversalLatest.note || ""}` : "等待数据");
  setText("reversalGoldValue", formatNumber(reversalLatest?.gold_price_usd_per_oz, 2));
  setText("reversalFxValue", formatNumber(reversalLatest?.usdcny_rate, 4));
  setText("reversalConditionsValue", reversalLatest?.triggered_conditions || "--");
  setText("us10ySignalValue", latestUs10y?.yield_signal ? "触发" : "无信号");
  setText("us10ySignalMeta", latestUs10y ? `${formatTime(latestUs10y.fetched_at)} | ${latestUs10y.note || ""}` : "等待数据");
  setText("us10yYieldValue", latestUs10y?.yield_pct != null ? `${formatNumber(latestUs10y.yield_pct, 3)}%` : "--");
  setText("us10ySourceValue", latestUs10y?.source || "--");
  setText("us10ySourceMeta", latestUs10y?.note || "等待数据");
  setText("us10yLinkValue", `${tenorLabel} ${(latestUs10y?.yield_signal ? "偏强" : "中性")}`);

  const signalBadge = document.getElementById("signalBadge");
  signalBadge.textContent = formatLevel(reversalLatest?.signal_level ?? 0);
  signalBadge.className = `signal-pill ${levelClass(reversalLatest?.signal_level ?? 0)}`;

  renderSgeTables();
  renderReversalTables();
  renderEvents();
  renderRuns();
  renderKeywordBoard();
  renderUs10yTables();
  renderNotificationLogs();
  renderRssMlPanel();
  renderRssDedupReport();
  if (!isEditingForm()) {
    fillForms(settings);
  }
  renderCharts();
}

function renderSgeTables() {
  const status = state.status;
  const samples = status.recent_samples || [];
  const alerts = status.recent_alerts || [];
  const samplesPage = getPagedItems(samples, "samples");
  const alertsPage = getPagedItems(alerts, "alerts");
  const overviewAlertsPage = getPagedItems(alerts, "overviewSgeAlerts");
  const samplesBody = document.getElementById("samplesBody");
  if (samplesBody) {
    samplesBody.innerHTML = samplesPage.items.map((item) => `
    <tr>
      <td>${formatTime(item.fetched_at)}</td>
      <td>${formatNumber(item.shfe_price_cny_per_g, 4)}</td>
      <td>${formatNumber(item.london_price_cny_per_g, 4)}</td>
      <td>${item.premium_cny_per_g != null ? `${formatNumber(item.premium_cny_per_g, 4)} 元/克` : "--"}</td>
      <td>${item.alert_triggered ? "已触发" : item.note || "正常"}</td>
    </tr>
  `).join("");
  }
  const alertsBody = document.getElementById("alertsBody");
  if (alertsBody) {
    alertsBody.innerHTML = alertsPage.items.map((item) => `
    <tr>
      <td>${formatTime(item.sent_at)}</td>
      <td>${formatNumber(item.premium_cny_per_g, 4)} 元/克</td>
      <td>${formatNumber(item.threshold_cny_per_g, 4)} 元/克</td>
      <td>${item.success ? "成功" : "失败"}</td>
    </tr>
  `).join("");
  }
  document.getElementById("overviewSgeAlerts").innerHTML = overviewAlertsPage.items.map((item) => `
    <tr>
      <td>${formatTime(item.sent_at)}</td>
      <td>${formatNumber(item.premium_cny_per_g, 4)} 元/克</td>
      <td>${item.success ? "成功" : "失败"}</td>
    </tr>
  `).join("") || `<tr><td colspan="3" class="muted">暂无记录</td></tr>`;
  renderPager("samplesPager", "samples", samples.length);
  renderPager("alertsPager", "alerts", alerts.length);
  renderPager("overviewSgeAlertsPager", "overviewSgeAlerts", alerts.length);
}

function renderReversalTables() {
  const reversalStatus = state.reversalStatus;
  const alerts = reversalStatus.recent_alerts || [];
  const history = [...state.reversalHistory].reverse().slice(0, 20);
  const samplesPage = getPagedItems(history, "reversalSamples");
  const alertsPage = getPagedItems(alerts, "reversalAlerts");
  const overviewAlertsPage = getPagedItems(alerts, "overviewReversalAlerts");
  document.getElementById("reversalSamplesBody").innerHTML = samplesPage.items.map((item) => `
    <tr>
      <td>${formatTime(item.fetched_at)}</td>
      <td>${formatNumber(item.gold_price_usd_per_oz, 2)}</td>
      <td><span class="tag ${levelClass(item.signal_level)}">${formatLevel(item.signal_level)}</span></td>
      <td>${item.triggered_conditions || "--"}</td>
      <td>${item.note || "--"}</td>
    </tr>
  `).join("");
  document.getElementById("reversalAlertsBody").innerHTML = alertsPage.items.map((item) => `
    <tr>
      <td>${formatTime(item.sent_at)}</td>
      <td><span class="tag ${levelClass(item.signal_level)}">${formatLevel(item.signal_level)}</span></td>
      <td>${item.triggered_conditions || "--"}</td>
      <td>${item.success ? "成功" : "失败"}</td>
    </tr>
  `).join("");
  document.getElementById("overviewReversalAlerts").innerHTML = overviewAlertsPage.items.map((item) => `
    <tr>
      <td>${formatTime(item.sent_at)}</td>
      <td>${formatLevel(item.signal_level)}</td>
      <td>${item.triggered_conditions || "--"}</td>
      <td>${item.success ? "成功" : "失败"}</td>
    </tr>
  `).join("") || `<tr><td colspan="4" class="muted">暂无记录</td></tr>`;
  renderPager("reversalSamplesPager", "reversalSamples", history.length);
  renderPager("reversalAlertsPager", "reversalAlerts", alerts.length);
  renderPager("overviewReversalAlertsPager", "overviewReversalAlerts", alerts.length);
}

function renderEvents() {
  const events = filterEvents(state.reversalStatus.recent_events || []);
  const overviewPage = getPagedItems(events, "overviewEvents");
  const feedPage = getPagedItems(events, "feedEvents");
  const geoPage = getPagedItems(events, "geoEvents");
  const mlLabels = ["\u5927\u5e45\u5229\u597d\u9ec4\u91d1", "\u5c0f\u5e45\u5229\u597d\u9ec4\u91d1", "\u5c0f\u5e45\u5229\u7a7a\u9ec4\u91d1", "\u5927\u5e45\u5229\u7a7a\u9ec4\u91d1"];
  const mlPoints = {
    "\u5927\u5e45\u5229\u597d\u9ec4\u91d1": 10,
    "\u5c0f\u5e45\u5229\u597d\u9ec4\u91d1": 7,
    "\u5c0f\u5e45\u5229\u7a7a\u9ec4\u91d1": 4,
    "\u5927\u5e45\u5229\u7a7a\u9ec4\u91d1": 1,
  };
  const mlClass = (label) => {
    if (label === "\u5927\u5e45\u5229\u597d\u9ec4\u91d1") return "high";
    if (label === "\u5c0f\u5e45\u5229\u597d\u9ec4\u91d1") return "mid";
    if (label === "\u5c0f\u5e45\u5229\u7a7a\u9ec4\u91d1") return "low";
    return "bear";
  };
  const parseMlProbs = (raw) => {
    if (!raw) return null;
    let value = raw;
    if (typeof raw === "string") {
      try {
        value = JSON.parse(raw);
      } catch {
        return null;
      }
    }
    if (!value || typeof value !== "object") return null;
    const out = {};
    let sum = 0;
    mlLabels.forEach((label) => {
      const prob = Number(value[label]);
      out[label] = Number.isFinite(prob) ? prob : 0;
      sum += out[label];
    });
    return sum > 0 ? out : null;
  };
  const buildMlHtml = (item) => {
    const probs = parseMlProbs(item.ml_class_probs);
    if (!probs) {
      return `<span class="tag risk-score low">\u56db\u5206\u7c7b\uff1a\u672a\u6253\u5206</span>`;
    }
    let topLabel = item.ml_bucket_label;
    if (!topLabel || !Object.prototype.hasOwnProperty.call(probs, topLabel)) {
      topLabel = mlLabels.reduce((acc, cur) => (probs[cur] > probs[acc] ? cur : acc), mlLabels[0]);
    }
    const chips = mlLabels
      .map((label) => {
        const active = label === topLabel ? "active" : "";
        return `<span class="tag ml-bucket ${mlClass(label)} ${active}">${label} ${formatNumber(probs[label] * 100, 1)}%</span>`;
      })
      .join("");
    return `
      <span class="tag risk-score ${mlClass(topLabel)}">\u6a21\u578b\u5206\u7c7b\uff1a${topLabel}\uff08${mlPoints[topLabel]}\u5206\uff09</span>
      ${chips}
    `;
  };
  const eventTypeLabel = (eventType) => {
    if (eventType === "political") return "\u653f\u6cbb\u7f13\u548c";
    if (eventType === "war") return "\u6218\u4e89\u8fdb\u5ea6";
    if (eventType === "general") return "\u5168\u91cf\u4e8b\u4ef6";
    return eventType || "\u672a\u77e5\u7c7b\u578b";
  };
  const buildHtml = (items, withScore = false) => items.length ? items.map((item) => {
    const eventType = item.event_type || "all";
    const keywords = (item.matched_keywords || "").split(",").filter(Boolean);
    return `
      <article class="event-card">
        <div class="event-meta">
          <span class="tag ${eventType}">${eventTypeLabel(eventType)}</span>
          <span class="tag">${item.source || "\u672a\u77e5\u6765\u6e90"}</span>
          <span class="tag">${formatTime(item.published_at || item.fetched_at)}</span>
          ${withScore ? buildMlHtml(item) : ""}
        </div>
        <h4>${item.title || "\u6682\u65e0\u6807\u9898"}</h4>
        <p>${item.summary || "\u6682\u65e0\u6458\u8981"}</p>
        <div class="event-meta">
          ${keywords.map((keyword) => `<span class="tag">${keyword}</span>`).join("")}
          ${item.link ? `<a href="${item.link}" target="_blank" rel="noreferrer">\u67e5\u770b\u539f\u6587</a>` : ""}
        </div>
      </article>
    `;
  }).join("") : `<div class="empty-state">\u6682\u65e0\u547d\u4e2d\u53cd\u8f6c\u6761\u4ef6\u7684 RSS \u4e8b\u4ef6</div>`;
  document.getElementById("overviewEvents").innerHTML = buildHtml(overviewPage.items, false);
  document.getElementById("feedEventList").innerHTML = buildHtml(feedPage.items, false);
  const geoList = document.getElementById("geoEventList");
  if (geoList) geoList.innerHTML = buildHtml(geoPage.items, true);
  renderPager("overviewEventsPager", "overviewEvents", events.length);
  renderPager("feedEventsPager", "feedEvents", events.length);
  renderPager("geoEventsPager", "geoEvents", events.length);
}

function renderRuns() {
  const marketState = state.status.market_state || {};
  const fetchRuns = state.status.recent_fetch_runs || [];
  const reversalRuns = state.reversalStatus.recent_runs || [];
  const rssFetchRuns = state.reversalStatus.recent_rss_fetch_runs || [];
  const rssRunsPage = getPagedItems(rssFetchRuns, "rssFetchRuns");

  if (marketState.sge?.active) {
    const fetchRunsPage = getPagedItems(fetchRuns, "fetchRuns");
    document.getElementById("fetchRunsList").innerHTML = fetchRunsPage.items.length ? fetchRunsPage.items.map((item) => `
      <li>
        <span>${formatTime(item.fetched_at)}</span>
        <span>${item.poll_interval_seconds}s / ${item.duration_ms}ms / ${item.success ? "成功" : item.error_message || "失败"}</span>
      </li>
    `).join("") : `<li class="muted">\u6682\u65e0\u8bb0\u5f55</li>`;
    renderPager("fetchRunsPager", "fetchRuns", fetchRuns.length);
  } else {
    document.getElementById("fetchRunsList").innerHTML = `<li class="muted">当前非 SGE 开盘时段，抓取任务暂不执行。</li>`;
    renderPager("fetchRunsPager", "fetchRuns", 0);
  }

  if (marketState.reversal?.active) {
    const reversalRunsPage = getPagedItems(reversalRuns, "reversalRuns");
    document.getElementById("reversalRunsList").innerHTML = reversalRunsPage.items.length ? reversalRunsPage.items.map((item) => `
      <li>
        <span>${formatTime(item.fetched_at)}</span>
        <span>${item.poll_interval_seconds}s / ${item.duration_ms}ms / RSS异常 ${item.rss_error_count}</span>
        <span>${item.success ? "\u6210\u529f" : item.error_message || "\u5931\u8d25"}</span>
      </li>
    `).join("") : `<li class="muted">\u6682\u65e0\u8bb0\u5f55</li>`;
    renderPager("reversalRunsPager", "reversalRuns", reversalRuns.length);
  } else {
    document.getElementById("reversalRunsList").innerHTML = `<li class="muted">当前未满足反转监控时段，任务暂不执行。</li>`;
    renderPager("reversalRunsPager", "reversalRuns", 0);
  }

  document.getElementById("rssFetchRunsList").innerHTML = rssRunsPage.items.length ? rssRunsPage.items.map((item) => `
    <li>
      <span>${formatTime(item.fetched_at)}</span>
      <span>${item.duration_ms}ms / 条目 ${item.item_count} / 异常 ${item.error_count}</span>
      <span>${item.success ? "\u6210\u529f" : item.error_message || "\u5931\u8d25"}</span>
    </li>
  `).join("") : `<li class="muted">\u6682\u65e0\u8bb0\u5f55</li>`;
  renderPager("rssFetchRunsPager", "rssFetchRuns", rssFetchRuns.length);
}

function renderNotificationLogs() {
  const logs = state.notificationLogs || [];
  const page = getPagedItems(logs, "notificationLogs");
  const rows = page.items.map((item) => `
    <tr>
      <td>${formatTime(item.sent_at)}</td>
      <td>${item.event_type || item.channel || "--"}</td>
      <td>${item.target_name || "--"}</td>
      <td>${item.success ? "成功" : "失败"}</td>
      <td title="${escapeHtml(item.response_text || item.content || "")}">${escapeHtml(item.content || item.response_text || "--")}</td>
    </tr>
  `).join("") || `<tr><td colspan="5" class="muted">暂无推送记录</td></tr>`;
  const body = document.getElementById("notificationLogsBody");
  if (body) body.innerHTML = rows;
  renderPager("notificationLogsPager", "notificationLogs", logs.length);
}

function renderUs10yTables() {
  const samples = state.us10yHistory || [];
  const runs = state.us10yStatus?.recent_runs || [];
  const sourceStatus = state.us10yStatus?.source_status || {};
  const samplesPage = getPagedItems([...samples].reverse(), "us10ySamples");
  const runsPage = getPagedItems(runs, "us10yRuns");

  const sampleBody = document.getElementById("us10ySamplesBody");
  if (sampleBody) {
    sampleBody.innerHTML = samplesPage.items.map((item) => `
      <tr>
        <td>${formatTime(item.fetched_at)}</td>
        <td>${(item.tenor || "--").toUpperCase()}</td>
        <td>${formatNumber(item.yield_pct, 3)}%</td>
        <td>${item.yield_signal ? "触发" : "无信号"}</td>
        <td>${item.note || "--"}</td>
      </tr>
    `).join("") || `<tr><td colspan="5" class="muted">暂无记录</td></tr>`;
  }

  const runsList = document.getElementById("us10yRunsList");
  if (runsList) {
    runsList.innerHTML = runsPage.items.map((item) => `
      <li>
        <span>${formatTime(item.fetched_at)}</span>
        <span>${item.duration_ms}ms / ${item.success ? "成功" : item.error_message || "失败"}</span>
      </li>
    `).join("") || `<li class="muted">暂无记录</li>`;
  }

  const sourceList = document.getElementById("us10ySourceStatusList");
  if (sourceList) {
    const tenorItems = Object.entries(sourceStatus);
    sourceList.innerHTML = tenorItems.length
      ? tenorItems
        .map(([tenor, checks]) => {
          const rows = (Array.isArray(checks) ? checks : [])
            .map((item) => {
              const ok = Boolean(item.ok);
              const age = Number(item.age_seconds);
              const ageText = Number.isFinite(age) && age >= 0 ? `${Math.round(age)}s` : "--";
              const latency = Number(item.latency_ms);
              const latencyText = Number.isFinite(latency) ? `${Math.round(latency)}ms` : "--";
              return `${item.source || "--"}:${ok ? "OK" : "FAIL"} value=${item.value ?? "--"} age=${ageText} latency=${latencyText} ${item.message || ""}`;
            })
            .join(" | ");
          return `
            <li>
              <span>${String(tenor).toUpperCase()}</span>
              <span>${rows || "--"}</span>
            </li>
          `;
        })
        .join("")
      : `<li class="muted">暂无信源状态（先执行一次采样）</li>`;
  }

  renderPager("us10ySamplesPager", "us10ySamples", samples.length);
  renderPager("us10yRunsPager", "us10yRuns", runs.length);
}

function renderRssMlPanel() {
  const status = state.rssMlStatus;
  if (!status) {
    setText("rssMlSampleCount", "--");
    setText("rssMlModelVersion", "--");
    setText("rssMlNextAutoTrain", "--");
    setText("rssMlLatestLoss", "--");
    const runsList = document.getElementById("rssMlRunsList");
    if (runsList) runsList.innerHTML = `<li class="muted">\u6682\u65e0\u8bad\u7ec3\u8bb0\u5f55</li>`;
    setText("rssMlTrainStateText", "\u5f85\u673a");
    setText("rssMlTrainStateTime", "--");
    setText("rssMlTrainStateDesc", "\u540e\u7aef\u72b6\u6001\u672a\u52a0\u8f7d\u3002");
    setText("rssMlTrainDetail", "--");
    const bar = document.getElementById("rssMlTrainProgressBar");
    if (bar) bar.style.width = "0%";
    renderRssMlMetrics(null);
    return;
  }

  const runtime = status.runtime || {};
  const sampleCount = status.training_sample_count ?? status.sample_count ?? 0;
  const fetchedCount = status.rss_event_count ?? 0;
  const fetchedScoredCount = status.rss_scored_event_count ?? 0;
  const modelVersion = status.model_version || "\u672a\u8bad\u7ec3";
  const latest = status.latest_training_run || null;
  const nextAuto = status.next_auto_train_at ?? "--";
  const source = status.train_data_source || "db";
  const macroF1 = Number(status.latest_macro_f1 ?? 0);
  const latestLoss = latest
    ? `Macro-F1 ${formatNumber(macroF1 * 100, 2)}% | loss train ${formatNumber(latest.train_loss, 4)} / val ${formatNumber(latest.val_loss, 4)}`
    : "\u6682\u65e0";
  setText(
    "rssMlSampleCount",
    `训练样本 ${sampleCount} (${source}) / 抓取事件 ${fetchedCount} / 已打分 ${fetchedScoredCount}`,
  );
  setText("rssMlModelVersion", modelVersion);
  setText("rssMlNextAutoTrain", `${nextAuto} \u6761`);
  setText("rssMlLatestLoss", latestLoss);

  const classDistribution = status.class_distribution || {};
  const classText = Object.entries(classDistribution)
    .map(([label, count]) => `${label}:${count}`)
    .join(" | ") || "--";

  const stepSize = Number(status?.config?.train_step_size || 100);
  const progressInStep = stepSize > 0 ? sampleCount % stepSize : 0;
  let progress = stepSize > 0 ? Math.max(0, Math.min(100, (progressInStep / stepSize) * 100)) : 0;
  if (runtime.running) {
    const rtEpoch = Number(runtime.epoch || 0);
    const rtMax = Number(runtime.max_epochs || 0);
    if (rtMax > 0) {
      progress = Math.max(0, Math.min(100, (rtEpoch / rtMax) * 100));
    }
  }
  const bar = document.getElementById("rssMlTrainProgressBar");
  if (bar) bar.style.width = `${progress.toFixed(1)}%`;

  if (!state.rssMlTrainUi.at) {
    if (latest?.trained_at) {
      state.rssMlTrainUi = {
        state: "\u5f85\u673a",
        desc: `\u6700\u8fd1\u8bad\u7ec3\uff1a\u6a21\u578b ${latest.model_version || "--"}\uff0c\u6837\u672c ${latest.sample_count || "--"}`,
        at: latest.trained_at,
        detail: `class_distribution=${classText}`,
      };
    } else {
      state.rssMlTrainUi = {
        state: "\u5f85\u673a",
        desc: "\u6682\u65e0\u8bad\u7ec3\u8bb0\u5f55\uff0c\u53ef\u5148\u5168\u91cf\u6293\u53d6\u540e\u518d\u8bad\u7ec3\u3002",
        at: new Date().toISOString(),
        detail: `class_distribution=${classText}`,
      };
    }
  }

  if (runtime.running) {
    let stateText = "\u8bad\u7ec3\u4e2d";
    if (runtime.cancel_requested || runtime.state === "cancelling") {
      stateText = "\u53d6\u6d88\u4e2d";
    } else if (runtime.paused || runtime.state === "paused" || runtime.state === "pausing") {
      stateText = "\u5df2\u6682\u505c";
    }
    setText("rssMlTrainStateText", stateText);
    setText("rssMlTrainStateTime", formatTime(runtime.started_at || new Date().toISOString()));
    setText("rssMlTrainStateDesc", runtime.message || `Epoch ${runtime.epoch || 0}/${runtime.max_epochs || 0}`);
    const runtimeDetail = runtime.error
      ? JSON.stringify(runtime.error, null, 2)
      : `epoch=${runtime.epoch || 0}\nmax_epochs=${runtime.max_epochs || 0}\nsample_count=${runtime.sample_count || 0}\nclass_distribution=${classText}`;
    setText("rssMlTrainDetail", runtimeDetail);
  } else {
    setText("rssMlTrainStateText", state.rssMlTrainUi.state || "\u5f85\u673a");
    setText("rssMlTrainStateTime", formatTime(state.rssMlTrainUi.at || new Date().toISOString()));
    setText("rssMlTrainStateDesc", state.rssMlTrainUi.desc || "\u7b49\u5f85\u8bad\u7ec3\u64cd\u4f5c\u3002");
    setText("rssMlTrainDetail", state.rssMlTrainUi.detail || `class_distribution=${classText}`);
  }

  const trainBtn = document.getElementById("trainRssMlBtn");
  const pauseBtn = document.getElementById("pauseRssMlTrainBtn");
  const resumeBtn = document.getElementById("resumeRssMlTrainBtn");
  const cancelBtn = document.getElementById("cancelRssMlTrainBtn");
  if (trainBtn) trainBtn.disabled = Boolean(runtime.running);
  if (pauseBtn) pauseBtn.disabled = !runtime.running || Boolean(runtime.paused) || Boolean(runtime.cancel_requested);
  if (resumeBtn) resumeBtn.disabled = !runtime.running || !runtime.paused || Boolean(runtime.cancel_requested);
  if (cancelBtn) cancelBtn.disabled = !runtime.running || Boolean(runtime.cancel_requested);

  const runs = status.recent_training_runs || [];
  const runsList = document.getElementById("rssMlRunsList");
  if (runsList) {
    runsList.innerHTML = runs.length
      ? runs.slice(0, 10).map((item) => `
        <li>
          <span>${formatTime(item.trained_at)}</span>
          <span>${item.model_version || "--"} / \u6837\u672c ${item.sample_count ?? "--"} / best@${item.best_epoch ?? "--"}</span>
          <span>train ${formatNumber(item.train_loss, 4)} / val ${formatNumber(item.val_loss, 4)}</span>
        </li>
      `).join("")
      : `<li class="muted">\u6682\u65e0\u8bad\u7ec3\u8bb0\u5f55</li>`;
  }

  renderRssMlMetrics(status);
}

function renderRssDedupReport() {
  const report = state.rssDedupReport;
  if (!report) {
    setText("rssDedupTime", "--");
    setText("rssDedupSummary", "\u5c1a\u672a\u6267\u884c\u624b\u52a8\u53bb\u91cd\u3002");
    setText("rssDedupDetail", "--");
    return;
  }
  const dedup = report.dedup || {};
  const csvSync = report.csvSync || {};
  const topSources = Array.isArray(dedup.top_duplicate_sources) ? dedup.top_duplicate_sources : [];
  const lines = [
    `removed_events=${dedup.removed_events ?? 0}`,
    `removed_samples=${dedup.removed_samples ?? 0}`,
    `dedup_groups=${dedup.dedup_groups ?? 0}`,
    `exact_removed_events=${dedup.exact_removed_events ?? 0}`,
    `fuzzy_removed_events=${dedup.fuzzy_removed_events ?? 0}`,
    `csv_rows=${csvSync.rows ?? "--"}`,
    `csv_near_removed=${csvSync.csv_near_removed ?? 0}`,
    "top_duplicate_sources:",
    ...(topSources.length
      ? topSources.slice(0, 10).map((item, idx) => `${idx + 1}. ${item.source || "unknown"}: ${item.count ?? 0}`)
      : ["(empty)"]),
  ];
  setText("rssDedupTime", formatTime(report.at));
  setText(
    "rssDedupSummary",
    `\u672c\u8f6e\u53bb\u91cd\uff1a\u4e8b\u4ef6 ${dedup.removed_events ?? 0} \u6761\uff0c\u6837\u672c ${dedup.removed_samples ?? 0} \u6761\uff0c\u91cd\u590d\u5206\u7ec4 ${dedup.dedup_groups ?? 0} \u7ec4\u3002`,
  );
  setText("rssDedupDetail", lines.join("\n"));
}

function renderRssMlMetrics(status) {
  const metricsWrap = document.getElementById("rssMlClassMetrics");
  const matrixWrap = document.getElementById("rssMlConfusionMatrix");
  if (!metricsWrap || !matrixWrap) return;

  if (!status) {
    metricsWrap.innerHTML = `<div class="empty-state">暂无分类指标</div>`;
    matrixWrap.innerHTML = `<div class="empty-state">暂无混淆矩阵</div>`;
    return;
  }

  const history = status.latest_epoch_history || {};
  const labels = Array.isArray(history.labels) ? history.labels : [];
  const classMetrics = Array.isArray(history.class_metrics) ? history.class_metrics : [];
  const confusion = Array.isArray(history.confusion_matrix) ? history.confusion_matrix : [];
  const macroF1 = classMetrics.length
    ? classMetrics.reduce((sum, item) => sum + Number(item.f1 || 0), 0) / classMetrics.length
    : 0;

  if (!classMetrics.length) {
    metricsWrap.innerHTML = `<div class="empty-state">暂无分类指标（先执行一次完整训练）</div>`;
  } else {
    metricsWrap.innerHTML = `
      <div class="ml-metrics-title">每类指标（Precision / Recall / F1） | Macro-F1: ${formatNumber(macroF1 * 100, 2)}%</div>
      <table class="ml-metrics-table">
        <thead>
          <tr>
            <th>类别</th>
            <th>Precision</th>
            <th>Recall</th>
            <th>F1</th>
            <th>样本数</th>
          </tr>
        </thead>
        <tbody>
          ${classMetrics
            .map(
              (item) => `
              <tr>
                <td>${item.label || "--"}</td>
                <td>${formatNumber((item.precision || 0) * 100, 2)}%</td>
                <td>${formatNumber((item.recall || 0) * 100, 2)}%</td>
                <td>${formatNumber((item.f1 || 0) * 100, 2)}%</td>
                <td>${item.support ?? 0}</td>
              </tr>`,
            )
            .join("")}
        </tbody>
      </table>
    `;
  }

  if (!labels.length || !confusion.length) {
    matrixWrap.innerHTML = `<div class="empty-state">暂无混淆矩阵（先执行一次完整训练）</div>`;
  } else {
    const header = labels.map((label) => `<th>${label}</th>`).join("");
    const rows = labels
      .map((label, i) => {
        const row = Array.isArray(confusion[i]) ? confusion[i] : [];
        return `<tr><th>${label}</th>${labels.map((_, j) => `<td>${row[j] ?? 0}</td>`).join("")}</tr>`;
      })
      .join("");
    matrixWrap.innerHTML = `
      <div class="ml-metrics-title">混淆矩阵（行=真实类别，列=预测类别）</div>
      <table class="ml-metrics-table">
        <thead><tr><th>真实\\预测</th>${header}</tr></thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  }
}

function setRssMlTrainUi(stateText, desc, detail = "--") {
  state.rssMlTrainUi = {
    state: stateText,
    desc: desc || "",
    at: new Date().toISOString(),
    detail: detail || "--",
  };
  renderRssMlPanel();
}

function renderRssMlTrainChart(chart) {
  if (!chart) return;
  const runtime = state.rssMlStatus?.runtime || {};
  const history = state.rssMlStatus?.latest_epoch_history || {};
  const trainCurve = runtime.running
    ? (Array.isArray(runtime.train_curve) ? runtime.train_curve : [])
    : (Array.isArray(history.train_curve) ? history.train_curve : []);
  const valCurve = runtime.running
    ? (Array.isArray(runtime.val_curve) ? runtime.val_curve : [])
    : (Array.isArray(history.val_curve) ? history.val_curve : []);
  const trainAccCurve = runtime.running
    ? (Array.isArray(runtime.train_acc_curve) ? runtime.train_acc_curve : [])
    : (Array.isArray(history.train_acc_curve) ? history.train_acc_curve : []);
  const valAccCurve = runtime.running
    ? (Array.isArray(runtime.val_acc_curve) ? runtime.val_acc_curve : [])
    : (Array.isArray(history.val_acc_curve) ? history.val_acc_curve : []);
  const hasEpochCurves = trainCurve.length > 0 && valCurve.length > 0;
  const runs = (state.rssMlStatus?.recent_training_runs || []).slice().reverse();
  const xData = hasEpochCurves
    ? trainCurve.map((_, idx) => `E${idx + 1}`)
    : runs.map((item) => item.trained_at || "");
  const trainData = hasEpochCurves ? trainCurve : runs.map((item) => item.train_loss);
  const valData = hasEpochCurves ? valCurve : runs.map((item) => item.val_loss);
  const trainAccData = hasEpochCurves
    ? trainAccCurve.map((v) => (v ?? 0) * 100)
    : runs.map((item) => (item.train_accuracy ?? 0) * 100);
  const valAccData = hasEpochCurves
    ? valAccCurve.map((v) => (v ?? 0) * 100)
    : runs.map((item) => (item.val_accuracy ?? 0) * 100);
  chart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis" },
    legend: { top: 6, textStyle: { color: "#5f6b7c" }, data: ["Train Loss", "Val Loss", "Train Acc", "Val Acc"] },
    grid: { left: 42, right: 18, top: 52, bottom: 40 },
    xAxis: {
      type: "category",
      axisLabel: {
        color: "#5f6b7c",
        formatter: (v) => (hasEpochCurves ? String(v) : String(v).slice(5, 16)),
      },
      axisLine: { lineStyle: { color: "#b8c1cb" } },
      data: xData,
    },
    yAxis: [
      {
        type: "value",
        name: "Loss",
        axisLabel: { color: "#5f6b7c" },
        splitLine: { lineStyle: { color: "rgba(18,32,51,0.08)" } },
      },
      {
        type: "value",
        name: "Accuracy %",
        min: 0,
        max: 100,
        axisLabel: { color: "#5f6b7c", formatter: (v) => `${v}%` },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: "Train Loss",
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: "#13796b" },
        data: trainData,
      },
      {
        name: "Val Loss",
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: "#bc4837" },
        data: valData,
      },
      {
        name: "Train Acc",
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: "#3a7bd5" },
        data: trainAccData,
      },
      {
        name: "Val Acc",
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: "#f39c12" },
        data: valAccData,
      },
    ],
  });
}

function renderKeywordBoard() {
  const events = state.reversalStatus.recent_events || [];
  const keywords = [...new Set(events.flatMap((item) => (item.matched_keywords || "").split(",").filter(Boolean)))];
  document.getElementById("eventKeywordList").innerHTML = keywords.length
    ? keywords.map((keyword) => `<span class="tag">${keyword}</span>`).join("")
    : `<div class="empty-state">暂无关键词</div>`;
}

function fillForms(settings) {
  const setValue = (id, value) => {
    const node = document.getElementById(id);
    if (node) node.value = value;
  };
  setValue("thresholdInput", settings.premium_threshold ?? 20);
  setValue("intervalInput", settings.poll_interval_seconds ?? 60);
  setValue("cooldownInput", settings.alert_cooldown_seconds ?? 900);
  setValue("timeoutInput", settings.request_timeout_seconds ?? 10);

  setValue("rssIntervalInput", settings.rss_poll_interval_seconds ?? 3600);
  setValue("reversalCooldownInput", settings.reversal_cooldown_seconds ?? 1800);
  setValue("reversalLookbackInput", settings.reversal_price_lookback_minutes ?? 360);
  setValue("reversalReboundInput", settings.reversal_price_rebound_pct ?? 1.2);
  setValue("reversalMaInput", settings.reversal_price_ma_window ?? 15);
  setValue("reversalSignalWindowInput", settings.reversal_signal_window_minutes ?? 180);
  setValue("us10yPollIntervalInput", settings.us10y_poll_interval_seconds ?? 60);
  setValue("us10yDropLookbackHoursInput", settings.us10y_drop_lookback_hours ?? 24);
  setValue("us10yDropThresholdBpInput", settings.us10y_drop_threshold_bp ?? 1.0);
  setValue("us10yAlertCooldownInput", settings.us10y_alert_cooldown_seconds ?? 1800);
  setValue("us10yAlertDedupHoursInput", settings.us10y_alert_dedup_hours ?? 4);
  const tenors = settings.us10y_tenors || ["10y"];
  const t5 = document.getElementById("us10yTenor5");
  const t10 = document.getElementById("us10yTenor10");
  const t20 = document.getElementById("us10yTenor20");
  if (t5) t5.checked = tenors.includes("5y");
  if (t10) t10.checked = tenors.includes("10y");
  if (t20) t20.checked = tenors.includes("20y");

  const mlConfig = state.rssMlStatus?.config || {};
  setValue("rssMlLearningRateInput", mlConfig.learning_rate ?? settings.rss_ml_learning_rate ?? 0.001);
  setValue("rssMlMaxEpochsInput", mlConfig.max_epochs ?? settings.rss_ml_max_epochs ?? 300);
  setValue("rssMlEarlyStopInput", mlConfig.early_stop_patience ?? settings.rss_ml_early_stop_patience ?? 25);
  setValue("rssMlTrainStepSizeInput", mlConfig.train_step_size ?? settings.rss_ml_train_step_size ?? 100);
  setValue("rssMlMinSamplesInput", mlConfig.min_train_samples ?? settings.rss_ml_min_train_samples ?? 30);
  setValue("rssMlWindowHoursInput", mlConfig.active_window_hours ?? settings.rss_ml_active_window_hours ?? 24);
  setValue("rssMlWeakMovePctInput", mlConfig.weak_move_pct ?? settings.rss_ml_weak_move_pct ?? 0.1);
  setValue("rssMlStrongMovePctInput", mlConfig.strong_move_pct ?? settings.rss_ml_strong_move_pct ?? 0.35);
  setValue("rssMlDecayHalfLifeInput", mlConfig.decay_half_life_hours ?? settings.rss_ml_decay_half_life_hours ?? 24);
  setValue("rssMlLabelModeInput", mlConfig.label_mode ?? settings.rss_ml_label_mode ?? "future_return");

  renderFeedRows(settings.rss_feeds || (settings.rss_feed_urls || []).map((url, idx) => ({
    name: `RSS源${idx + 1}`,
    url,
    enabled: true,
  })));
  renderTargetRows(settings.notification_targets || []);
}

function isEditingForm() {
  const active = document.activeElement;
  return active instanceof HTMLElement && Boolean(active.closest("form"));
}

function renderFeedRows(items) {
  const container = document.getElementById("feedRows");
  container.innerHTML = "";
  const feeds = items.length ? items : [{ name: "RSS源1", url: "", enabled: true }];
  feeds.forEach((feed, index) => {
    const safeFeed = typeof feed === "string"
      ? { name: `RSS源${index + 1}`, url: feed, enabled: true }
      : feed;
    container.appendChild(createFeedRow(normalizeFeedItem(safeFeed, index), index));
  });
}

function normalizeFeedItem(feed, index = 0) {
  const url = String(feed?.url || "").trim();
  let name = String(feed?.name || "").trim();
  if (!name || isLikelyGarbledText(name)) {
    if (url.includes("huxiu.com")) name = "虎嗅";
    else if (url.includes("quanwenrss.com/bloomberg")) name = "彭博英文财经";
    else if (url.includes("beehiiv.com")) name = "Beehiiv宏观";
    else if (url.includes("GM3DSNZUGJ6DOYTEG5RWENZRGUZDENLD")) name = "金十快讯A";
    else if (url.includes("GM4DKMRUG56DIYZTHE2TAY3EGNTDAML")) name = "金十快讯B";
    else name = `RSS源${index + 1}`;
  }
  return {
    name,
    url,
    enabled: feed?.enabled !== false,
  };
}

function createFeedRow(feed = {}, index = 0) {
  const node = document.getElementById("feedRowTemplate").content.firstElementChild.cloneNode(true);
  node.querySelector(".feed-name-input").value = feed.name || `RSS源${index + 1}`;
  node.querySelector(".feed-url-input").value = feed.url || "";
  node.querySelector(".feed-enabled-input").checked = feed.enabled !== false;
  node.querySelector(".remove-feed-btn").addEventListener("click", () => {
    if (document.querySelectorAll("#feedRows .editor-row").length > 1) {
      node.remove();
    } else {
      node.querySelector(".feed-name-input").value = "RSS源1";
      node.querySelector(".feed-url-input").value = "";
      node.querySelector(".feed-enabled-input").checked = true;
    }
  });
  return node;
}

function renderTargetRows(items) {
  const container = document.getElementById("targetRows");
  container.innerHTML = "";
  const targets = items.length ? items : [{ name: "默认推送组", webhook: "", secret: "", enabled: true }];
  targets.forEach((target) => container.appendChild(createTargetRow(target)));
}

function createTargetRow(target = {}) {
  const node = document.getElementById("targetRowTemplate").content.firstElementChild.cloneNode(true);
  node.querySelector(".target-name-input").value = target.name || "默认推送组";
  node.querySelector(".target-webhook-input").value = target.webhook || "";
  node.querySelector(".target-secret-input").value = target.secret || "";
  node.querySelector(".target-enabled-input").checked = target.enabled !== false;
  node.querySelector(".remove-target-btn").addEventListener("click", () => {
    if (document.querySelectorAll("#targetRows .editor-row").length > 1) {
      node.remove();
    } else {
      node.querySelector(".target-name-input").value = "默认推送组";
      node.querySelector(".target-webhook-input").value = "";
      node.querySelector(".target-secret-input").value = "";
      node.querySelector(".target-enabled-input").checked = true;
    }
  });
  return node;
}

function renderCharts() {
  renderSgeChart(state.charts.sgeChart);
  renderSgeChart(state.charts.sgeDetailChart);
  renderReversalChart(state.charts.reversalChart);
  renderReversalChart(state.charts.reversalDetailChart);
  renderUs10yLinkChart(state.charts.us10yLinkChart);
  renderRssMlTrainChart(state.charts.rssMlTrainChart);
}

function renderSgeChart(chart) {
  if (!chart) return;
  const items = state.sgeHistory;
  const times = items.map((item) => item.fetched_at);
  const yMin = state.sgeYAxisRange?.min ?? 800;
  const yMax = state.sgeYAxisRange?.max ?? 1200;
  chart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis" },
    legend: { top: 6, textStyle: { color: "#5f6b7c" }, data: ["溢价", "人民币金价", "国际金折算"] },
    grid: { left: 42, right: 18, top: 52, bottom: 56 },
    xAxis: { type: "time", axisLabel: { color: "#5f6b7c" }, axisLine: { lineStyle: { color: "#b8c1cb" } } },
    yAxis: [
      {
        type: "value",
        min: yMin,
        max: yMax,
        axisLabel: { color: "#5f6b7c" },
        splitLine: { lineStyle: { color: "rgba(18,32,51,0.08)" } },
      },
      {
        type: "value",
        axisLabel: { color: "#5f6b7c" },
        splitLine: { show: false },
      },
    ],
    dataZoom: [{ type: "inside", filterMode: "none", zoomOnMouseWheel: false }],
    series: [
      {
        name: "溢价",
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 3, color: "#c04f2d" },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: "rgba(192,79,45,0.28)" },
            { offset: 1, color: "rgba(192,79,45,0.02)" },
          ]),
        },
        data: times.map((time, index) => [time, items[index].premium_cny_per_g]),
      },
      {
        name: "人民币金价",
        type: "line",
        yAxisIndex: 0,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: "#13796b" },
        data: times.map((time, index) => [time, items[index].shfe_price_cny_per_g]),
      },
      {
        name: "国际金折算",
        type: "line",
        yAxisIndex: 0,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: "#d3a132" },
        data: times.map((time, index) => [time, items[index].london_price_cny_per_g]),
      },
    ],
  });
}

function renderReversalChart(chart) {
  if (!chart) return;
  const items = state.reversalHistory;
  const times = items.map((item) => item.fetched_at);
  const mapSignalToY = (level) => {
    if (level === 1) return 4;
    if (level === 2) return 3;
    if (level === 3) return 2;
    if (level === 4) return 1;
    return 0.2;
  };

  chart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis" },
    legend: { top: 6, textStyle: { color: "#5f6b7c" }, data: ["现货金价格", "反转信号"] },
    grid: { left: 42, right: 18, top: 52, bottom: 56 },
    xAxis: { type: "time", axisLabel: { color: "#5f6b7c" }, axisLine: { lineStyle: { color: "#b8c1cb" } } },
    yAxis: [
      {
        type: "value",
        name: "美元/盎司",
        axisLabel: { color: "#5f6b7c" },
        splitLine: { lineStyle: { color: "rgba(18,32,51,0.08)" } },
      },
      {
        type: "value",
        name: "反转等级",
        min: 0,
        max: 4.2,
        interval: 1,
        axisLabel: {
          color: "#5f6b7c",
          formatter: (value) => {
            if (value === 4) return "一级";
            if (value === 3) return "二级";
            if (value === 2) return "三级";
            if (value === 1) return "四级";
            if (value === 0) return "无信号";
            return "";
          },
        },
        splitLine: { show: false },
      },
    ],
    dataZoom: [{ type: "inside", filterMode: "none" }],
    series: [
      {
        name: "现货金",
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 3, color: "#13796b" },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: "rgba(19,121,107,0.24)" },
            { offset: 1, color: "rgba(19,121,107,0.02)" },
          ]),
        },
        data: times.map((time, index) => [time, items[index].gold_price_usd_per_oz]),
      },
      {
        name: "反转信号",
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: "#bc4837" },
        itemStyle: { color: "#bc4837" },
        data: times.map((time, index) => [time, mapSignalToY(items[index].signal_level)]),
      },
    ],
  });
}

function renderUs10yLinkChart(chart) {
  if (!chart) return;
  const goldItems = state.reversalHistory || [];
  const us10yHistory = state.us10yHistory || [];
  const settingsTenors = state.status?.settings?.us10y_tenors || [];
  const presentTenors = [...new Set(us10yHistory.map((item) => (item.tenor || "10y").toLowerCase()))];
  const baseOrder = ["5y", "10y", "20y"];
  const tenorCandidates = settingsTenors.length ? settingsTenors : presentTenors;
  const tenors = baseOrder.filter((tenor) => tenorCandidates.includes(tenor));
  const finalTenors = tenors.length ? tenors : ["10y"];
  const us10yItemsByTenor = Object.fromEntries(
    finalTenors.map((tenor) => [tenor, us10yHistory.filter((item) => (item.tenor || "10y").toLowerCase() === tenor)]),
  );
  const tenorColor = {
    "5y": "#7f56d9",
    "10y": "#c04f2d",
    "20y": "#1d4ed8",
  };
  const tenorSeries = finalTenors.map((tenor) => {
    const items = us10yItemsByTenor[tenor] || [];
    return {
      name: `美债${tenor.toUpperCase()}`,
      type: "line",
      yAxisIndex: 1,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 2, color: tenorColor[tenor] || "#c04f2d" },
      data: items.map((item) => [item.fetched_at, item.yield_pct]),
    };
  });

  chart.setOption({
    backgroundColor: "transparent",
    tooltip: {
      trigger: "axis",
      triggerOn: "mousemove|click",
      axisPointer: { type: "cross", snap: false },
      formatter: (params) => {
        const axisValue = Array.isArray(params) && params.length ? params[0].axisValue : null;
        return buildUs10yTooltip(axisValue, goldItems, us10yItemsByTenor, finalTenors, tenorColor);
      },
    },
    legend: {
      top: 6,
      textStyle: { color: "#5f6b7c" },
      data: ["黄金", ...finalTenors.map((tenor) => `美债${tenor.toUpperCase()}`)],
    },
    grid: { left: 42, right: 18, top: 52, bottom: 56 },
    xAxis: { type: "time", axisLabel: { color: "#5f6b7c" }, axisLine: { lineStyle: { color: "#b8c1cb" } } },
    yAxis: [
      {
        type: "value",
        name: "美元/盎司",
        axisLabel: { color: "#5f6b7c" },
        splitLine: { lineStyle: { color: "rgba(18,32,51,0.08)" } },
      },
      {
        type: "value",
        name: "%",
        axisLabel: { color: "#5f6b7c" },
        splitLine: { show: false },
      },
    ],
    dataZoom: [
      { type: "inside", filterMode: "none" },
      { type: "inside", filterMode: "none", yAxisIndex: [0, 1], zoomOnMouseWheel: true },
    ],
    series: [
      {
        name: "黄金",
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2.5, color: "#13796b" },
        data: goldItems.map((item) => [item.fetched_at, item.gold_price_usd_per_oz]),
      },
      ...tenorSeries,
    ],
  });
}

function filterEvents(events) {
  if (state.eventFilter === "all") return events;
  return events.filter((item) => item.event_type === state.eventFilter);
}

function switchView(view) {
  state.activeView = view;
  document.querySelectorAll(".menu-btn").forEach((button) => {
    button.classList.toggle("active", button.dataset.view === view);
  });
  document.querySelectorAll(".view").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.viewPanel === view);
  });
  const meta = viewMeta[view] || viewMeta.overview;
  setText("heroTitle", meta.title);
  setText("heroDesc", meta.desc);
  window.dispatchEvent(new Event("resize"));
}

function syncRangeButtons() {
  document.querySelectorAll(".range-btn").forEach((button) => {
    const chart = button.dataset.chart;
    const activeRange = chart === "reversal"
      ? state.reversalRange
      : chart === "us10y"
        ? state.us10yRange
        : state.sgeRange;
    button.classList.toggle("active", button.dataset.range === activeRange);
  });
}

function syncEventFilterButtons() {
  document.querySelectorAll(".filter-btn").forEach((button) => {
    button.classList.toggle("active", button.dataset.eventFilter === state.eventFilter);
  });
}

async function saveSgeSettings(event) {
  event.preventDefault();
  const payload = {
    premium_threshold: Number(document.getElementById("thresholdInput").value),
    poll_interval_seconds: Number(document.getElementById("intervalInput").value),
    alert_cooldown_seconds: Number(document.getElementById("cooldownInput").value),
    request_timeout_seconds: Number(document.getElementById("timeoutInput").value),
  };
  try {
    await fetchJson("/api/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await refreshAll();
    showToast("保存成功", `已更新 SGE 设置：阈值 ${payload.premium_threshold} 元/克，频率 ${payload.poll_interval_seconds}s。`);
  } catch (error) {
    showToast("保存失败", `SGE 设置更新失败：${formatErrorMessage(error)}`, "error");
  }
}

async function saveReversalSettings(event) {
  event.preventDefault();
  const payload = {
    reversal_cooldown_seconds: Number(document.getElementById("reversalCooldownInput").value),
    reversal_price_lookback_minutes: Number(document.getElementById("reversalLookbackInput").value),
    reversal_price_rebound_pct: Number(document.getElementById("reversalReboundInput").value),
    reversal_price_ma_window: Number(document.getElementById("reversalMaInput").value),
    reversal_signal_window_minutes: Number(document.getElementById("reversalSignalWindowInput").value),
  };
  try {
    await fetchJson("/api/reversal/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await refreshAll();
    showToast("保存成功", "反转参数已更新。");
  } catch (error) {
    showToast("执行失败", `执行反转监控失败：${formatErrorMessage(error)}`, "error");
  }
}

async function saveFeedSettings(event) {
  event.preventDefault();
  const rssFeeds = [...document.querySelectorAll("#feedRows .editor-row")]
    .map((row, idx) => ({
      name: row.querySelector(".feed-name-input")?.value?.trim() || `RSS源${idx + 1}`,
      url: row.querySelector(".feed-url-input")?.value?.trim() || "",
      enabled: Boolean(row.querySelector(".feed-enabled-input")?.checked),
    }))
    .filter((item) => item.url);
  const enabledCount = rssFeeds.filter((item) => item.enabled).length;
  const payload = {
    rss_feeds: rssFeeds,
    rss_feed_urls: rssFeeds.filter((item) => item.enabled).map((item) => item.url),
    rss_poll_interval_seconds: Number(document.getElementById("rssIntervalInput").value),
  };
  try {
    await fetchJson("/api/reversal/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await refreshAll();
    showToast("保存成功", `已保存 ${rssFeeds.length} 个源（启用 ${enabledCount} 个）。`);
  } catch (error) {
    showToast("保存失败", `RSS 配置更新失败：${formatErrorMessage(error)}`, "error");
  }
}

async function saveNotificationSettings(event) {
  event.preventDefault();
  const targets = [...document.querySelectorAll("#targetRows .editor-row")].map((row) => ({
    name: row.querySelector(".target-name-input").value.trim() || "\u9ed8\u8ba4\u63a8\u9001\u7ec4",
    webhook: row.querySelector(".target-webhook-input").value.trim(),
    secret: row.querySelector(".target-secret-input").value.trim(),
    enabled: row.querySelector(".target-enabled-input").checked,
  })).filter((item) => item.webhook);

  const firstTarget = targets[0] || { webhook: "", secret: "" };
  try {
    await fetchJson("/api/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        notification_targets: targets,
        dingtalk_webhook: firstTarget.webhook,
        dingtalk_secret: firstTarget.secret,
      }),
    });
    await refreshAll();
    showToast("保存成功", `已保存 ${targets.length} 个推送组。`);
  } catch (error) {
    showToast("保存失败", `参数更新失败：${formatErrorMessage(error)}`, "error");
  }
}

async function saveUs10ySettings(event) {
  event.preventDefault();
  const tenors = [];
  if (document.getElementById("us10yTenor5")?.checked) tenors.push("5y");
  if (document.getElementById("us10yTenor10")?.checked) tenors.push("10y");
  if (document.getElementById("us10yTenor20")?.checked) tenors.push("20y");
  if (!tenors.length) tenors.push("10y");
  const payload = {
    us10y_poll_interval_seconds: Number(document.getElementById("us10yPollIntervalInput").value),
    us10y_drop_lookback_hours: Number(document.getElementById("us10yDropLookbackHoursInput").value),
    us10y_drop_threshold_bp: Number(document.getElementById("us10yDropThresholdBpInput").value),
    us10y_alert_cooldown_seconds: Number(document.getElementById("us10yAlertCooldownInput").value),
    us10y_alert_dedup_hours: Number(document.getElementById("us10yAlertDedupHoursInput").value),
    us10y_tenors: tenors,
  };
  try {
    await fetchJson("/api/reversal/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await refreshAll();
    showToast(
      "保存成功",
      `已保存美债参数：${tenors.join(", ").toUpperCase()} / ${payload.us10y_drop_lookback_hours}h / ${payload.us10y_drop_threshold_bp}bp / 冷却${payload.us10y_alert_cooldown_seconds}s`,
    );
  } catch (error) {
    showToast("保存失败", `美债参数保存失败：${formatErrorMessage(error)}`, "error");
  }
}

async function saveRssMlConfig(event) {
  event.preventDefault();
  const payload = {
    rss_ml_learning_rate: Number(document.getElementById("rssMlLearningRateInput").value),
    rss_ml_max_epochs: Number(document.getElementById("rssMlMaxEpochsInput").value),
    rss_ml_early_stop_patience: Number(document.getElementById("rssMlEarlyStopInput").value),
    rss_ml_train_step_size: Number(document.getElementById("rssMlTrainStepSizeInput").value),
    rss_ml_min_train_samples: Number(document.getElementById("rssMlMinSamplesInput").value),
    rss_ml_active_window_hours: Number(document.getElementById("rssMlWindowHoursInput").value),
    rss_ml_weak_move_pct: Number(document.getElementById("rssMlWeakMovePctInput").value),
    rss_ml_strong_move_pct: Number(document.getElementById("rssMlStrongMovePctInput").value),
    rss_ml_decay_half_life_hours: Number(document.getElementById("rssMlDecayHalfLifeInput").value),
    rss_ml_label_mode: String(document.getElementById("rssMlLabelModeInput").value || "future_return"),
  };
  try {
    await fetchJson("/api/rss-ml/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await refreshAll();
    showToast("保存成功", `模型参数已更新：lr=${payload.rss_ml_learning_rate}, epochs=${payload.rss_ml_max_epochs}`);
  } catch (error) {
    showToast("保存失败", `模型参数更新失败：${formatErrorMessage(error)}`, "error");
  }
}

async function runRssMlTrain() {
  setRssMlTrainUi("启动中", "正在创建训练任务…", "等待后端返回");
  try {
    const result = await fetchJson("/api/rss-ml/train", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ force: true }),
    });
    if (result.status) {
      state.rssMlStatus = result.status;
      renderRssMlPanel();
      renderRssMlTrainChart(state.charts.rssMlTrainChart);
    }
    const syncRows = Number(result?.csv_db_sync?.synced_samples ?? 0);
    const syncMsg = `CSV->DB ${syncRows} rows`;
    if (result.started) {
      setRssMlTrainUi("训练中", result.message || "训练任务已启动", syncMsg);
      showToast("训练启动", `训练任务已启动，${syncMsg}`);
      startRssMlTrainPolling();
    } else {
      setRssMlTrainUi("启动失败", result.message || "训练任务启动失败", JSON.stringify(result.status || {}, null, 2).slice(0, 1200));
      showToast("启动失败", result.message || "训练任务启动失败", "error");
    }
  } catch (error) {
    setRssMlTrainUi("请求失败", formatErrorMessage(error), String(error?.message || error));
    showToast("请求失败", `训练请求失败：${formatErrorMessage(error)}`, "error");
  }
}

async function pollRssMlTrainStatusOnce() {

  const data = await fetchJson("/api/rss-ml/train-status");
  if (data?.status) {
    state.rssMlStatus = data.status;
    renderRssMlPanel();
    renderRssMlTrainChart(state.charts.rssMlTrainChart);
  }
  const runtime = data?.runtime || {};
  if (!runtime.running) {
    stopRssMlTrainPolling();
    const stateText = runtime.state || "idle";
    if (stateText === "completed") {
      const latest = data?.status?.latest_training_run || {};
      const macroF1 = Number(data?.status?.latest_macro_f1 || 0);
      const trainCurve = data?.status?.latest_epoch_history?.train_curve;
      const epochsRan = Array.isArray(trainCurve) ? trainCurve.length : Number(runtime.epoch || 0);
      setRssMlTrainUi(
        "训练成功",
        runtime.message || "训练完成",
        `model=${latest.model_version || "--"}\nmacro_f1=${formatNumber(macroF1 * 100, 4)}%\ntrain_loss=${formatNumber(latest.train_loss, 6)}\nval_loss=${formatNumber(latest.val_loss, 6)}\nepochs_ran=${epochsRan || "--"}\nbest_epoch=${latest.best_epoch || "--"}`,
      );
      showToast("训练完成", runtime.message || "训练完成");
    } else if (stateText === "cancelled") {
      setRssMlTrainUi("已取消", runtime.message || "训练已取消", JSON.stringify(runtime.error || {}, null, 2).slice(0, 1200));
      showToast("训练已取消", runtime.message || "训练已取消", "error");
    } else if (stateText === "failed") {
      setRssMlTrainUi("训练失败", runtime.message || "训练失败", JSON.stringify(runtime.error || {}, null, 2).slice(0, 1200));
      showToast("训练失败", runtime.message || "训练失败", "error");
    } else {
      setRssMlTrainUi("未触发", runtime.message || "未触发训练", JSON.stringify(runtime, null, 2).slice(0, 1200));
    }
  }
}

function startRssMlTrainPolling() {
  stopRssMlTrainPolling();
  state.rssMlPollTimer = window.setInterval(() => {
    pollRssMlTrainStatusOnce().catch(() => {});
  }, 1000);
  pollRssMlTrainStatusOnce().catch(() => {});
}

function stopRssMlTrainPolling() {
  if (state.rssMlPollTimer) {
    window.clearInterval(state.rssMlPollTimer);
    state.rssMlPollTimer = null;
  }
}

async function controlRssMlTrain(action) {
  try {
    const result = await fetchJson("/api/rss-ml/train-control", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action }),
    });
    if (result?.runtime) {
      if (!state.rssMlStatus) state.rssMlStatus = {};
      state.rssMlStatus.runtime = result.runtime;
      renderRssMlPanel();
      renderRssMlTrainChart(state.charts.rssMlTrainChart);
    }

    if (action === "cancel") {
      setRssMlTrainUi("取消中", result.message || "已发送取消请求", JSON.stringify(result.runtime || {}, null, 2).slice(0, 1200));
      startRssMlTrainPolling();
    } else if (action === "pause") {
      setRssMlTrainUi("已暂停", result.message || "已发送暂停请求", JSON.stringify(result.runtime || {}, null, 2).slice(0, 1200));
      startRssMlTrainPolling();
    } else if (action === "resume") {
      setRssMlTrainUi("已恢复", result.message || "训练已恢复", JSON.stringify(result.runtime || {}, null, 2).slice(0, 1200));
      startRssMlTrainPolling();
    }

    showToast("训练控制", result.message || action);
  } catch (error) {
    showToast("训练控制失败", formatErrorMessage(error), "error");
  }
}

async function clearRssMlSamples() {
  const confirmed = window.confirm("确认清空模型样本和训练记录吗？此操作不可恢复。");
  if (!confirmed) return;
  const btn = document.getElementById("clearRssMlSamplesBtn");
  if (btn) btn.disabled = true;
  setRssMlTrainUi("清理中", "正在清空样本与训练记录…", "--");
  try {
    stopRssMlTrainPolling();
    const result = await fetchJson("/api/rss-ml/clear-samples", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ remove_model_file: true }),
    });
    if (result.status) state.rssMlStatus = result.status;
    await refreshAll();
    const info = result.result || {};
    const detail = `deleted_samples=${info.deleted_samples || 0}\ndeleted_training_runs=${info.deleted_training_runs || 0}\ncleared_event_ml_fields=${info.cleared_event_ml_fields || 0}\nremoved_model_file=${info.removed_model_file ? "yes" : "no"}`;
    setRssMlTrainUi("已清空", "样本与训练记录已清空。", detail);
    showToast("清空完成", `样本 ${info.deleted_samples || 0} 条，训练记录 ${info.deleted_training_runs || 0} 条。`);
  } catch (error) {
    setRssMlTrainUi("清理失败", formatErrorMessage(error), String(error?.message || error));
    showToast("清空失败", `清空样本失败：${formatErrorMessage(error)}`, "error");
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function syncRssMlCsv(overwrite = false) {
  const btn = document.getElementById("syncRssMlCsvBtn");
  if (btn) btn.disabled = true;
  try {
    const result = await fetchJson("/api/rss-ml/sync-csv", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ overwrite }),
    });
    if (result.status) {
      state.rssMlStatus = result.status;
      renderRssMlPanel();
    }
    const csvResult = result.result || {};
    showToast("同步完成", `CSV: ${csvResult.path || "--"} / rows=${csvResult.rows ?? 0}`);
  } catch (error) {
    showToast("同步失败", `CSV同步失败：${formatErrorMessage(error)}`, "error");
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function runRssBulkFill() {
  const rounds = Number(document.getElementById("rssBulkRoundsInput")?.value || 3);
  const includeUnmatched = Boolean(document.getElementById("rssBulkIncludeUnmatchedInput")?.checked);
  const btn = document.getElementById("bulkFillRssBtn");
  if (btn) btn.disabled = true;
  setRssMlTrainUi("抓取中", `正在执行 ${rounds} 轮全量抓取…`);
  try {
    const result = await fetchJson("/api/reversal/rss-bulk-fill", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        rounds,
        include_unmatched: includeUnmatched,
        use_extended_sources: false,
      }),
    });
    await refreshAll();
    setRssMlTrainUi("抓取完成", `轮数 ${result.rounds}，新增 ${result.total_items}，错误 ${result.total_errors}`);
    showToast("抓取完成", `轮数 ${result.rounds}，新增 ${result.total_items}，错误 ${result.total_errors}`);
  } catch (error) {
    setRssMlTrainUi("抓取失败", formatErrorMessage(error));
    showToast("抓取失败", `全量抓取失败：${formatErrorMessage(error)}`, "error");
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function runRssDedup() {
  const btn = document.getElementById("runRssDedupBtn");
  if (btn) btn.disabled = true;
  setRssMlTrainUi("\u53bb\u91cd\u4e2d", "\u6b63\u5728\u6267\u884c RSS \u8bed\u4e49\u53bb\u91cd\u5e76\u540c\u6b65 CSV...", "--");
  try {
    const result = await fetchJson("/api/reversal/rss-dedup", { method: "POST" });
    state.rssDedupReport = {
      at: new Date().toISOString(),
      dedup: result.dedup || {},
      csvSync: result.csv_sync || {},
    };
    if (result.status) {
      state.rssMlStatus = result.status;
    }
    renderRssDedupReport();
    renderRssMlPanel();
    const dedup = result.dedup || {};
    const csvRows = result.csv_sync?.rows ?? "--";
    const csvNearRemoved = result.csv_sync?.csv_near_removed ?? 0;
    setRssMlTrainUi(
      "\u53bb\u91cd\u5b8c\u6210",
      `\u4e8b\u4ef6\u53bb\u91cd ${dedup.removed_events ?? 0} \u6761\uff0c\u6837\u672c\u53bb\u91cd ${dedup.removed_samples ?? 0} \u6761\u3002`,
      `csv_rows=${csvRows}\ncsv_near_removed=${csvNearRemoved}`,
    );
    showToast(
      "\u53bb\u91cd\u5b8c\u6210",
      `\u4e8b\u4ef6 ${dedup.removed_events ?? 0} \u6761\uff0c\u6837\u672c ${dedup.removed_samples ?? 0} \u6761\uff0cCSV \u989d\u5916\u53bb\u91cd ${csvNearRemoved} \u6761\u3002`,
    );
  } catch (error) {
    setRssMlTrainUi("\u53bb\u91cd\u5931\u8d25", formatErrorMessage(error), String(error?.message || error));
    showToast("\u53bb\u91cd\u5931\u8d25", `\u624b\u52a8\u53bb\u91cd\u5931\u8d25\uff1a${formatErrorMessage(error)}`, "error");
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function runUs10yMonitor() {
  try {
    await fetchJson("/api/us10y/run-once", { method: "POST" });
    await refreshAll();
    showToast("执行成功", "已立即采样并评估美债收益率。");
  } catch (error) {
    showToast("执行失败", `美债采样失败：${formatErrorMessage(error)}`, "error");
  }
}

async function runAllMonitors() {
  try {
    await fetchJson("/api/run-once", { method: "POST" });
    await refreshAll();
    showToast("执行成功", "已立即执行 SGE + 反转 + RSS。");
  } catch (error) {
    showToast("执行失败", `执行 SGE + 反转 + RSS 失败：${formatErrorMessage(error)}`, "error");
  }
}

async function runReversalMonitor() {
  try {
    await fetchJson("/api/reversal/run-once", { method: "POST" });
    await refreshAll();
    showToast("执行成功", "已立即执行反转监控。");
  } catch (error) {
    showToast("执行失败", `执行反转监控失败：${formatErrorMessage(error)}`, "error");
  }
}

async function runRssMonitor() {
  try {
    await fetchJson("/api/reversal/rss-run-once", { method: "POST" });
    await refreshAll();
    showToast("执行成功", "已立即执行 RSS 抓取。");
  } catch (error) {
    showToast("执行失败", `RSS 抓取失败：${formatErrorMessage(error)}`, "error");
  }
}

async function sendTestAlert() {
  const level = Number(document.getElementById("testAlertLevel").value);
  const note = document.getElementById("testAlertNote").value.trim();
  try {
    const result = await fetchJson("/api/reversal/test-alert", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ level, note }),
    });
    showToast(
      result.success ? "推送成功" : "推送失败",
      result.success ? `已发送${formatLevel(level)}测试推送` : result.response_text,
      result.success ? "success" : "error",
    );
  } catch (error) {
    showToast("推送失败", `测试推送失败：${formatErrorMessage(error)}`, "error");
  }
}

function bindEvents() {
  const bindClick = (id, handler) => {
    const node = document.getElementById(id);
    if (node) node.addEventListener("click", handler);
  };
  const bindSubmit = (id, handler) => {
    const node = document.getElementById(id);
    if (node) node.addEventListener("submit", handler);
  };
  document.addEventListener("click", (event) => {
    const button = event.target.closest(".pager-btn[data-pager]");
    if (!button || button.disabled) return;
    const key = button.dataset.pager;
    const page = Number(button.dataset.page);
    if (!key || Number.isNaN(page) || page < 1) return;
    state.pagers[key] = page;
    renderAll();
  });

  document.querySelectorAll(".menu-btn").forEach((button) => {
    button.addEventListener("click", () => {
      const view = button.dataset.view;
      if (view === "goldWarning") {
        switchView("sge");
        return;
      }
      switchView(view);
    });
  });

  document.querySelectorAll(".range-btn").forEach((button) => {
    button.addEventListener("click", async () => {
      if (button.dataset.chart === "reversal") {
        state.reversalRange = button.dataset.range;
      } else if (button.dataset.chart === "us10y") {
        state.us10yRange = button.dataset.range;
      } else {
        state.sgeRange = button.dataset.range;
      }
      syncRangeButtons();
      await refreshAll();
    });
  });

  document.querySelectorAll(".filter-btn").forEach((button) => {
    button.addEventListener("click", () => {
      state.eventFilter = button.dataset.eventFilter;
      syncEventFilterButtons();
      renderEvents();
    });
  });

  bindSubmit("sgeSettingsForm", saveSgeSettings);
  bindSubmit("reversalSettingsForm", saveReversalSettings);
  bindSubmit("us10ySettingsForm", saveUs10ySettings);
  bindSubmit("rssMlConfigForm", saveRssMlConfig);
  bindSubmit("feedSettingsForm", saveFeedSettings);
  bindSubmit("notificationSettingsForm", saveNotificationSettings);

  bindClick("runAllBtn", runAllMonitors);
  bindClick("runReversalBtn", runReversalMonitor);
  bindClick("runUs10yBtn", runUs10yMonitor);
  bindClick("trainRssMlBtn", runRssMlTrain);
  bindClick("pauseRssMlTrainBtn", () => controlRssMlTrain("pause"));
  bindClick("resumeRssMlTrainBtn", () => controlRssMlTrain("resume"));
  bindClick("cancelRssMlTrainBtn", () => controlRssMlTrain("cancel"));
  bindClick("syncRssMlCsvBtn", () => syncRssMlCsv(false));
  bindClick("clearRssMlSamplesBtn", clearRssMlSamples);
  bindClick("bulkFillRssBtn", runRssBulkFill);
  bindClick("runRssDedupBtn", runRssDedup);
  bindClick("fetchRssBtn", runRssMonitor);
  bindClick("refreshBtn", refreshAll);
  bindClick("testAlertBtn", sendTestAlert);
  bindClick("updateLogBtn", openUpdateLogModal);
  bindClick("closeUpdateLogBtn", closeUpdateLogModal);
  bindClick("addUpdateLogEntryBtn", () => {
    const text = window.prompt("请输入更新记录内容");
    if (!text || !text.trim()) return;
    appendUpdateLog(text.trim());
  });
  bindClick("clearUpdateLogBtn", () => {
    const confirmed = window.confirm("\u786e\u8ba4\u6e05\u7a7a\u66f4\u65b0\u8bb0\u5f55\u5417\uff1f");
    if (!confirmed) return;
    state.updateLogs = [];
    saveUpdateLogs();
    renderUpdateLogList();
  });
  document.querySelectorAll("[data-close-update-log]").forEach((node) => {
    node.addEventListener("click", closeUpdateLogModal);
  });

  bindClick("addFeedBtn", () => {
    const count = document.querySelectorAll("#feedRows .editor-row").length;
    document.getElementById("feedRows").appendChild(
      createFeedRow({ name: `RSS源${count + 1}`, url: "", enabled: true }, count),
    );
  });
  bindClick("addTargetBtn", () => {
    document.getElementById("targetRows").appendChild(createTargetRow({ name: "默认推送组", enabled: true }));
  });

  window.addEventListener("resize", () => {
    Object.values(state.charts).forEach((chart) => chart.resize());
  });
}

state.updateLogs = loadUpdateLogs();
ensurePresetUpdateLogs();
renderUpdateLogList();
setText("appVersionText", APP_VERSION);

initCharts();
bindEvents();
syncRangeButtons();
syncEventFilterButtons();
switchView("overview");

refreshAll().catch((error) => {
  setText("heroDesc", error.message);
});

setInterval(() => {
  refreshAll().catch(() => {});
}, 15000);
