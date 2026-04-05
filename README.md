# KiraAI_sustained_chat_plugin/可持续聊天
基于kiraai2.6.1原版 Default Chat 的修改版本，包含原版的功能和逻辑下新增功能：

1.优化非唤醒消息识别：修改原版开启上下文收听后默认所有图片、合并转发消息都识别的逻辑，减轻小水管识图模型的负担。默认开启只有明确唤醒（如at、关键词和引用回复时的消息中带有的）的图片和转发消息才会被识别。

2.增加持续回复功能：AI 回复后，在设定时间内收到非唤醒消息可按概率再次回复。必须开启Receive Unmentioned Messages开关（默认已开启）。

安装方法：根据个人喜好可采取两种方式

方式一：复制文件夹内容替换KiraAI-main\core\plugin\builtin_plugins\chat文件夹下内容，即直接替代原版Default Chat插件。

方式二：复制文件夹到KiraAI-main\data\plugins路径下，但必须webui里关闭原版Default Chat插件或更旧版的Message Debounce插件以免冲突。
