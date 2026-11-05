# مخطط البيانات بعد المراجعة

الاختيار هو **Star Schema في ClickHouse** للقياسات والتحليل، وPostgreSQL ببيانات تشغيلية مطبّعة لإدارة المزارع والمناطق والمحاصيل والحساسات. Bronze/Silver/Gold هي مراحل جودة ومعالجة؛ لا يلزم تحويل كل مرحلة إلى star. توجد مخططات Spark صريحة لجميع مسارات القراءة والكتابة، وDDL صريح لجداول ClickHouse.

هذا الاختيار يناسب تاريخ القياسات بحسب المزرعة والمنطقة والحساس والمحصول، ويتيح حساب مؤشرات واضحة. Snowflake سيزيد joins بين كيانات صغيرة دون فائدة لازمة هنا، وData Vault أكبر من احتياجات هذا المشروع. المخطط النجمي لا يغني عن عقود إدخال البيانات أو منع التكرار.

## الأبعاد والحقائق

| الكيان | حبيبة السجل / المفتاح | المصدر |
|---|---|---|
| `dim_farm` | مزرعة واحدة / farm_id | PostgreSQL farms |
| `dim_zone` | منطقة واحدة / zone_id؛ تتضمن farm_id وcrop_id | PostgreSQL zones |
| `dim_crop` | محصول واحد / crop_id | PostgreSQL crops |
| `dim_sensor` | حساس واحد / sensor_id | PostgreSQL sensors |
| `fact_sensor_readings` | قراءة واحدة / المزرعة والمنطقة والحساس وevent_time وreading_id | Silver المقبول |
| `fact_weather_observations` | مزرعة + توقيت ملاحظة المزود + weather_reading_id | Weather Silver |
| `fact_zone_summary` | view لملخص المنطقة عند وقت آخر حدث | zone_summary_history |
| `fact_farm_weather_daily` | view لمزرعة ويوم UTC | farm_weather_daily |
| `ml_predictions` | نتيجة واحدة لمنطقة وملاحظة وإصدار نموذج؛ prediction_id ثابت | ML |

الأبعاد **SCD Type 1**: تعرض metadata الحالية وتستبدل ذريًا عبر ClickHouse EXCHANGE TABLES. مفاتيح crop_id/farm_id/zone_id محفوظة في حقيقة القراءة نفسها، فلا يعتمد تاريخ القياس على علاقة المنطقة الحالية فقط. إذا احتجت تاريخ ملكية المزرعة أو أسماء المحاصيل أو نقل الحساسات، يلزم إضافة SCD Type 2 بمفاتيح surrogate وفترات صلاحية؛ هذه النسخة لا تدّعي حفظ ذلك التاريخ. الزمن محفوظ UTC؛ نستخرج اليوم والشهر من event_time دون جدول تاريخ مكرر حاليًا.

جداول `*_latest` و`*_history` الحالية هي marts مهيأة للعرض، تحتفظ بالأسماء لتقليل joins في Grafana. هذه إضافة إلى حقائق التحليل الأساسية وليست بيانات بلا schema. كل متوسط في latest مبني على أحدث قراءة لكل حساس خلال عشر دقائق من أحدث حدث، وليس عدد مرات وصول الحساس داخل microbatch. حالة الحساس القديم محفوظة لكنها لا تدخل متوسط الحالة الحية. `dry_readings` و`healthy_readings` و`overwatered_readings` في ملخص الحالة تعني عدد الحساسات الحديثة بحسب أحدث حالة، وأسماؤها القديمة محفوظة للتوافق.

## عقود الطبقات

| الطبقة | المحتوى والأنواع | التقسيم |
|---|---|---|
| Bronze Sensor | عقد JSON v1 + raw_value + Kafka topic/partition/offset/timestamp + event_time typed UTC | year/month/day من الحدث |
| Silver Sensor | IDs، أسماء، 4 قياسات Float64، version، reading_id، timestamp، event_time، processing_time، حدود المحصول وماء موصى به nullable، water_status | year/month/day |
| Gold Sensor | حالة حساس واحدة؛ ملخصات مناطق ومزارع؛ سجل تنبيهات Dry/Overwatered | snapshots مستقلة + مجلد تاريخ لكل batch |
| Bronze Weather | schema v2: هوية المزرعة، ملاحظة المزود، وقت الجلب، حرارة ورطوبة وضغط ورياح وسحاب ومطر مرصود، وصف الطقس | year/month/day |
| Silver Weather | ملاحظات مطبّعة UTC ومزالة التكرار بحسب المزرعة ووقت المزود | تاريخ الملاحظة |
| Gold Weather | مزرعة/يوم، وملاحظات طقس حالية، وتنبيهات بمطر مرصود وتوفر الأدلة | عقود منفصلة معرفة في contracts.py |

عقد الحساس الخارجي في `configs/contracts/sensor-reading-v1.schema.json`؛ version 1 اختياري فقط لدعم رسائل المحاكي القديمة ويصبح 1 عند التخزين. مسارات `_v2` تعني إصدار تخزين جديد، وليست تغييرًا لنسخة رسالة الحساس. الأنواع المقبولة IDs أعداد صحيحة موجبة ضمن Int64، القياسات finite، النسب 0–100، الحرارة −10–70°C، والتوقيت ست خانات microseconds مع Z. هذا مجال قبول تقني موحد، وليس توصية زراعية لكل محصول.

قبل Bronze يُرفض JSON غير صالح أو مكرر المفاتيح، القيم المفقودة، الأنواع الخاطئة، أسماء/معرّفات لا تطابق PostgreSQL، timestamps غير صالحة أو أكثر من خمس دقائق في المستقبل. المرفوض يذهب إلى `quarantine/sensor_ingestion` مع raw_value وسبب الرفض وKafka coordinates. المرفوض الدفاعي في Silver يذهب إلى `quarantine/sensor_enrichment`. الطقس المرفوض يذهب إلى `quarantine/weather`. لا يمر أي رفض جديد إلى Bronze/Silver/Gold.

سجل الحساسات وقواعد الري يُقرأ عند بداية stream: إعادة التشغيل مطلوبة بعد تعديل metadata أو إضافة حساس/محصول. القواعد المفقودة/غير الصالحة تؤدي إلى Unknown_NoRule دون توصية عددية أو استنتاج ML بأنها حالة صحية. watermark الحساس 10 دقائق؛ الأحداث المتأخرة أكثر منه قد تُسقط من Silver، لذا إعادة تحميل التاريخ تكون job batch منفصلة، وليست إعادة استعمال checkpoint الحي.

## المعنى الذي يجوز عرضه

- الحرارة °C، الرطوبة %، المطر المرصود mm، الرياح من OpenWeather m/s وتُحوّل في API إلى km/h.
- المطر اليومي تقدير من أخذ عينة واحدة لكل ساعة UTC من rain_1h؛ لا يمثل gauge تراكميًا كاملًا، ولا توقع مطر. توقع الغد واحتمال المطر `null` إلى أن يُضاف مصدر forecast فعلي.
- API يعرض حالة unknown وmoisture null إذا تجاوز آخر قياس 10 دقائق، و503 للطقس غير المتاح أو الأقدم من ساعتين. البطارية وآخر ري وحالة الصمام غير متاحة لغياب telemetry حقيقي.
- ML نموذج للجفاف الحالي بعلامات مشتقة من حدود المحصول، وليس إثباتًا لتوقع احتياج الماء غدًا. التقييم زمني، والطقس اليومي المستقبلي مستبعد من backtest. confidence = احتمال الفئة المختارة؛ احتمال الجفاف مستقل داخل feature_snapshot. rule fallback حتمي؛ ثقته تعني حتمية القاعدة لا دقة نموذج مقاسة.
- كمية الري الموصى بها mm ليست ماءً مستهلكًا فعليًا. لإظهار الاستهلاك والتوفير وتأكيد تشغيل المضخة يلزم flow meter وأحداث actuator مع timestamp والمساحة.

## إعادة التنفيذ

حقائق القياسات والنتائج تستخدم ReplacingMergeTree بمفاتيح مستقرة ونسخة زمنية. تاريخ ملخص المنطقة/المزرعة يحتفظ بأحدث revision عند نفس last_update، مع snapshot_id = batch_id + 1 لتفضيل التصحيح الأحدث عند وصول قراءة متأخرة ومنع retry قديم من استبداله. لا يُحفظ كل revision وسيط كسجل تدقيق مستقل. Views وقراءات التحليل تستخدم FINAL لأن إزالة التكرار الخلفية غير فورية. snapshots المنشورة في MinIO تستخدم version Parquet مستقلًا ثم manifest صغيرًا؛ راجع `snapshot-publication.md`. يوجد كاتب واحد لكل dataset. لا توجد transaction مشتركة بين Kafka وMinIO وClickHouse؛ retries وآثار النشر الجزئي يعالَجان بالمفاتيح الثابتة وcheckpoints، ويجب اختبار kill/restart حيًا. لا تصفّر checkpoint Gold مع إبقاء الجداول الحالية لأن batch_id سيعود للبداية.

التوثيق المرجعي: [ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)، [Spark foreachBatch](https://spark.apache.org/docs/3.5.0/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch)، [OpenWeather Current Weather](https://openweathermap.org/current).
