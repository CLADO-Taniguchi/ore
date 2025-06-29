# n8n Marketing Cloud to Snowflake 移行ノートブック自動生成ワークフロー - 完成版

## プロジェクト概要

Marketing CloudのクエリをSnowflakeに移行するため、既に最適化されたクエリを自動的にノートブック形式でGitHubリポジトリに保存するワークフローを作成。

## 完成したワークフロー構成

### 1. **Snowflake Query Node** - データ取得
```sql
SELECT 
    automation_name_en,
    CONCAT(
        '-- ====================================', CHAR(10),
        '-- ', automation_name_en, ' Migration Notebook', CHAR(10),
        '-- Generated: ', CURRENT_TIMESTAMP()::STRING, CHAR(10),
        '-- Total Steps: ', COUNT(step_order)::STRING, CHAR(10),
        '-- ====================================', CHAR(10), CHAR(10),
        LISTAGG(
            CONCAT(
                '-- Step ', step_order::STRING, ': ', step_name_en, CHAR(10),
                '-- Target: ', target_table, ' (', data_action, ')', CHAR(10),
                '-- Optimization: ', LEFT(optimization_note, 200), CHAR(10), CHAR(10),
                optimized_query, ';', CHAR(10), CHAR(10),
                '-- ================================================', CHAR(10), CHAR(10)
            ),
            ''
        ) WITHIN GROUP (ORDER BY step_order)
    ) as notebook_content
FROM mc_automation_studios a
JOIN mc_automation_steps s ON a.id = s.automation_id
WHERE conversion_status = 'SUCCESS'
GROUP BY automation_name_en
ORDER BY automation_name_en;
```

### 2. **Code Node 1** - ノートブック整形
```javascript
// ノートブックファイル生成処理（最終版）
const results = [];
for (const item of $input.all()) {
    const automation = item.json.AUTOMATION_NAME_EN;
    const content = item.json.NOTEBOOK_CONTENT; // 改行変換不要
    
    // ファイル名の生成
    const timestamp = new Date().toISOString().split('T')[0];
    const safeAutomationName = automation.replace(/[^a-zA-Z0-9_-]/g, '_');
    const filename = `${safeAutomationName}_notebook_${timestamp}.sql`;
    
    // メタデータ計算
    const lines = content.split('\n');
    const fileSizeKB = Math.round(content.length / 1024 * 100) / 100;
    const stepCount = (content.match(/-- Step \d+:/g) || []).length;
    
    results.push({
        automation_name: automation,
        filename: filename,
        content: content,
        file_size_bytes: content.length,
        file_size_kb: fileSizeKB,
        line_count: lines.length,
        step_count: stepCount,
        created_at: new Date().toISOString(),
        download_ready: true,
        preview: content.substring(0, 200) + '...',
        first_lines: lines.slice(0, 5).join('\n')
    });
}

return results;
```

### 3. **Code Node 2** - Base64エンコード
```javascript
// Base64エンコード処理
const results = [];

for (const item of $input.all()) {
    // JavaScriptの標準機能でBase64エンコード
    const base64Content = btoa(unescape(encodeURIComponent(item.json.content)));
    
    results.push({
        ...item.json,
        content_base64: base64Content  // Base64エンコード済みコンテンツを追加
    });
}

return results;
```

### 4. **Loop Over Items (Split in Batches) Node**
- **Batch Size**: `1`
- 複数のAutomationファイルを1つずつ順次処理

### 5. **HTTP Request Node** - GitHub API連携

#### 設定:
- **Method**: `PUT`
- **URL**: `https://api.github.com/repos/CLADO-Taniguchi/ore/contents/{{ $json.filename }}`

#### Headers:
```json
{
  "Authorization": "token ghp_YOUR_CLASSIC_TOKEN",
  "Accept": "application/vnd.github+json",
  "User-Agent": "n8n-migration-tool"
}
```

#### Body (JSON):
```json
{
  "message": "Auto-generated migration notebook: {{ $json.filename }}",
  "content": "{{ $json.content_base64 }}",
  "branch": "main"
}
```

## 重要な設定ポイント

### GitHub認証
- **Classic Personal Access Token** を使用
- **Fine-grained Token** では一部のAPI制限あり
- 必要なスコープ: `repo` (フルアクセス)
- 認証ヘッダー形式: `token YOUR_TOKEN` (Bearerではない)

### ファイル処理
- Snowflakeから取得したコンテンツは改行変換不要
- Base64エンコードが必須（GitHub API要件）
- ファイル名の安全化（特殊文字をアンダースコアに変換）

### トラブルシューティング
1. **404 エラー** → 認証ヘッダーの形式確認
2. **Buffer エラー** → n8nでは使用不可、btoaを使用
3. **Fine-grained Token制限** → Classic Tokenに変更

## 実行結果

### 成功例:
- ✅ ファイル作成: `ESM_DELETED_TABLE_INTEGRATION_notebook_2025-06-29.sql`
- ✅ GitHubリポジトリ: `https://github.com/CLADO-Taniguchi/ore`
- ✅ コミットメッセージ: "Auto-generated migration notebook"
- ✅ HTTP Status: 201 (Created)

### 生成されるノートブック内容:
```sql
-- ====================================
-- ESM_DELETED_TABLE_INTEGRATION Migration Notebook
-- Generated: 2025-06-29 00:01:45.086 -0700
-- Total Steps: 12
-- ====================================

-- Step 1: CUSTOMER_ITEM_DELETION_ADD
-- Target: CustomerSheet (Update)
-- Optimization: **最適化サマリー:** **変更した主要ポイント:** 1. **CTEによる前処理分離**...

WITH customer_delete_processed AS (
  SELECT 
    ID as original_id,
    CASE 
      WHEN POSITION...
```

## 拡張可能な機能

### Phase 2: 追加機能（実装可能）
1. **Schedule Trigger** - 定期実行（毎日/毎週）
2. **Slack/Teams通知** - 実行完了報告
3. **エラーハンドリング** - 失敗時の再試行
4. **複数リポジトリ対応** - 組織別保存
5. **Notion API連携** - ドキュメント化

### Slack通知例:
```json
{
  "text": "✅ Snowflake Migration Notebooks Updated",
  "blocks": [
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "*Snowflake Migration Notebooks Updated*\n\n📊 *Total Notebooks:* {{ $('Loop Over Items').itemMatching(0).length }}\n⏰ *Updated:* {{ new Date().toISOString() }}\n🔗 *View in GitHub*"
      }
    }
  ]
}
```

## データベース構造

### 使用テーブル:
- `mc_automation_studios` - Automation情報
- `mc_automation_steps` - ステップ詳細
- 条件: `conversion_status = 'SUCCESS'`

### 主要フィールド:
- `automation_name_en` - Automation名
- `step_order` - ステップ順序
- `step_name_en` - ステップ名
- `target_table` - 対象テーブル
- `data_action` - データ操作種別
- `optimization_note` - 最適化ノート
- `optimized_query` - 最適化済みクエリ

## 運用上の注意点

1. **トークン管理**: 定期的な更新とセキュリティ確保
2. **ファイル容量**: 大量クエリ時のGitHub制限
3. **実行頻度**: API rate limit考慮
4. **バックアップ**: 重要データの複製保存

## 成果

- ✅ 手動作業の完全自動化
- ✅ バージョン管理機能
- ✅ チーム間での共有促進
- ✅ ドキュメント品質の標準化
- ✅ 移行作業の効率化

このワークフローにより、Marketing CloudからSnowflakeへの移行作業が大幅に効率化され、チーム全体での作業品質が向上しました。